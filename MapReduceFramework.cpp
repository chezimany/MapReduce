//
// Created by cheziunix on 02/06/2021.
//

#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include "Barrier.h"
#include <semaphore.h>


struct JobContext;
struct ThreadContext;
/**
 * a comparator to compare between pairs of pointers to K1, V1.
 * @param a - pair of pointers to K1, V1.
 * @param b - pair of pointers to K1, V1.
 * @return - true if a.V1 < b.V2.
 */
bool compareIntermediatePairs(IntermediatePair  a, IntermediatePair  b)
{
    return (*a.first < *b.first);
}

/**
 * A struct to save each MapReduce job progress details.
 */
typedef struct JobContext
{
    InputVec inputVec;
    std::vector<IntermediateVec> victor;
    OutputVec *outputVec{};
    stage_t stage;
    std::atomic<long unsigned int>* pairsProcessed{};
    std::atomic<long unsigned int>* totalMappedPairs{};
    std::atomic<bool>* isWaitedFor{};
    pthread_mutex_t* waitForMutex{};
    pthread_mutex_t* switchFazeMutex{};
    std::vector<pthread_t*> threadsVec;
    std::vector<ThreadContext *> threadContexts;
}JobContext;

/**
 * A struct to hold relevant data for each thread.
 */
typedef struct ThreadContext
{
    IntermediateVec intermediateVec;
    std::atomic<long unsigned int>* idxCounter;
    std::atomic<long unsigned int>* totalMappedPairs;
    std::atomic<long unsigned int>* pairsProcessed;
    Barrier* barrier;
    const MapReduceClient* client;
    sem_t* shuffleSemaphore;
    bool* isShuffled;
    JobContext* job;
    pthread_mutex_t* pushBackMutex;
}ThreadContext;


/**
 * A function for each thread to run the map-reduce algorithm.
 * receive inputs from the client via a JobContext struct of the process
 * update the state of the job on processed pairs
 * @param arg
 * @return
 */
void *mapReduce(void* arg);

/**
 * Function to print different errors to the user
 */
void printErr(const std::string& str);

/**
 * Function to initialize a job Context pointer of the process
 * @param inputVec input vector (from Client)
 * @param outputVec output vector (from client)
 * @return pointer to job context.
 */
JobContext * initJob(const InputVec &inputVec,
                     OutputVec *outputVec,
                     std::atomic<long unsigned int>* pairsProcessed,
                     std::atomic<long unsigned int>* totalMappedPairs,
                     std::atomic<bool>* isWaitFor,
                     pthread_mutex_t* waitForMutex, pthread_mutex_t* switchFazeMutex);

/**
 * Fnction to initialize a thread context
 * @param inputVec input vector (from Client)
 * @param barrier pointer to Barrier object (before the shuffle phase)
 * @param client Client object
 * @param job JobContext pointer
 */
void initTrdContext(Barrier *barrier,
                    const MapReduceClient &client, JobContext* job,
                    std::atomic<long unsigned int> * idxCounter,
                    std::atomic<long unsigned int> * totalMappedPairs,
                    sem_t* shuffleSemaphore, bool* shufflePointer,
                    pthread_mutex_t* pushBackMutex,
                    std::atomic<long unsigned int>* pairsProcessed);

/**
 * Function to find the maximum key in all intermediate vectors
 * @param job jobContext object
 * @return pair with the maximum value.
 */
IntermediatePair findMax(std::vector<ThreadContext *>& threadContexts);

/**
 * given a max pair, extract all the appearances of the pair from the inter mediate vectors.
 * @param pair - pair that it's key has the highest value currently.
 * @param threadContexts - thread contexts holding each thread's intermediate vec.
 * @return - a vector holding all the pair appearances with the same key as the given pair.
 */
IntermediateVec extractMax(IntermediatePair pair, std::vector<ThreadContext *>& threadContexts);

/**
 * A function to shuffle the (K2, V2) pairs.
 * @param victor - vector to hold vectors of pairs.
 * @param threadContexts - contexts of threads holding the vectors of (K2, V2) pairs.
 */
void shuffle (std::vector<IntermediateVec>& victor,
              std::vector<ThreadContext *>& threadContexts);

/**
 * frees all the memory allocated for a given thread context.
 * @param context
 */
void freeContextResources(ThreadContext* context);

void emit2(K2 *key, V2 *value, void *context)
{
    auto * trdContext = static_cast<ThreadContext *>(context);
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    trdContext->intermediateVec.push_back(pair);
    (*trdContext->totalMappedPairs)++;
}

void emit3(K3 *key, V3 *value, void *context)
{
    auto * trdContext = static_cast<ThreadContext *>(context);
    OutputPair pair;
    pair.first = key;
    pair.second = value;
    int err = pthread_mutex_lock(trdContext->pushBackMutex);
    if (err) printErr("Failed to lock mutex;");
    trdContext->job->outputVec->push_back(pair);
    err = pthread_mutex_unlock(trdContext->pushBackMutex);
    if (err) printErr("Failed to unlock mutex;");
}

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                  OutputVec &outputVec, int multiThreadLevel)
{

    auto * barrier = new(std::nothrow) Barrier(multiThreadLevel);
    auto * idxCounter = new(std::nothrow) std::atomic<long unsigned int>;
    auto * totalMappedPairs = new(std::nothrow) std::atomic<long unsigned int>;
    auto * isWaitFor = new(std::nothrow) std::atomic<bool>;
    auto * shuffleSem = new(std::nothrow) sem_t;
    auto * shuffledPointer = new (std::nothrow) bool;
    auto * pushBackMutex = new (std::nothrow) pthread_mutex_t;
    auto * switchFazeMutex = new (std::nothrow) pthread_mutex_t;
    auto * waitForMutex = new (std::nothrow) pthread_mutex_t;
    auto *pairsProcessed = new(std::nothrow) std::atomic<long unsigned int>;
    int sem = sem_init(shuffleSem, 0, 1); // return 0 on success, -1 otherwise
    int ptrd_init = pthread_mutex_init(pushBackMutex, nullptr);
    int ptrd_init2 = pthread_mutex_init(waitForMutex, nullptr);
    int ptrd_init3 = pthread_mutex_init(switchFazeMutex, nullptr);
    if (!barrier || !idxCounter || !totalMappedPairs || !shuffleSem || sem
        || !shuffledPointer || !pushBackMutex || ptrd_init || !pairsProcessed || !waitForMutex ||
        !isWaitFor|| !switchFazeMutex || ptrd_init2 || ptrd_init3)
    {
        printErr("Failed to allocate memory for the job.");
    }
    *idxCounter = 0;
    *totalMappedPairs = 0;
    *pairsProcessed = 0;
    *shuffledPointer = false;
    *isWaitFor = false;

    JobContext *job = initJob(inputVec, &outputVec, pairsProcessed, totalMappedPairs, isWaitFor, waitForMutex, switchFazeMutex);

    job->stage = MAP_STAGE;
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        initTrdContext(barrier, client, job, idxCounter,
                       totalMappedPairs, shuffleSem, shuffledPointer, pushBackMutex, pairsProcessed);
    }

    return job;
}

void waitForJob(JobHandle job)
{
    auto * jobContext = static_cast<JobContext *>(job);
    int err = pthread_mutex_lock(jobContext->waitForMutex);
    if (err)
    {
        printErr("Failed to lock mutex.");
    }

    if (*jobContext->isWaitedFor)
    {
        err = pthread_mutex_unlock(jobContext->waitForMutex);
        if (err)
        {
            printErr("Failed to unlock mutex.");
        }
        return;
    }

    for (auto thread: jobContext->threadsVec)
    {
        pthread_join(*thread, nullptr);
    }
    *jobContext->isWaitedFor = true;
    err = pthread_mutex_unlock(jobContext->waitForMutex);
    if (err)
    {
        printErr("Failed to unlock mutex.");
    }
}

void getJobState(JobHandle job, JobState *state)
{

    auto * jobContext = static_cast<JobContext *>(job);
    int err = pthread_mutex_lock(jobContext->switchFazeMutex);
    if (err) printErr("Failed to lock mutex");
    state->stage = jobContext->stage;
    auto moneh = (float) *(jobContext->pairsProcessed), mehane1 = (float)jobContext->inputVec.size(),
    mehane2 = (float)*jobContext->totalMappedPairs, mehane3 = (float)jobContext->victor.size();

    float mehane;
    switch (state->stage)
    {
        case MAP_STAGE:
            mehane = mehane1;
            break;
        case SHUFFLE_STAGE:
            mehane = mehane2;
            break;
        case REDUCE_STAGE:
            mehane = mehane3;
            break;
        default:
            // UNDEFINED
            state->percentage = 100;
            return;
    }
    float temp = (moneh / mehane) * 100 ;
    state->percentage = temp < 100 ? temp : 100;
    err = pthread_mutex_unlock(jobContext->switchFazeMutex);
    if (err) printErr("Failed to lock mutex");
}

void closeJobHandle(JobHandle job)
{
    auto * jobContext = static_cast<JobContext *>(job);
    waitForJob(jobContext);
    delete jobContext->isWaitedFor;
    delete jobContext->waitForMutex;
    delete jobContext->switchFazeMutex;
    auto firstContext = jobContext->threadContexts.at(0);
    freeContextResources(firstContext);
    for (ThreadContext* context: jobContext->threadContexts)
    {
        delete context;
    }
    for (auto thread: jobContext->threadsVec)
    {
        delete thread;
    }
    delete jobContext;
}

void *mapReduce(void* arg)
{

    auto * trdContext = static_cast<ThreadContext *>(arg);
    // 1st LOOP
    // Get pair from input, run map on it & insert to local vector
    while (*(trdContext->idxCounter) < trdContext->job->inputVec.size())
    {
        long unsigned int idx = (*(trdContext->idxCounter))++;
        if (idx >= trdContext->job->inputVec.size()) break;
        trdContext->client->map(trdContext->job->inputVec.at(idx).first,
                                trdContext->job->inputVec.at(idx).second, trdContext);
        (*trdContext->pairsProcessed)++;
    }
    // Sort local vector
    std::sort((trdContext->intermediateVec).begin(),
              (trdContext->intermediateVec).end(),
              compareIntermediatePairs);
    // Barrier
    trdContext->barrier->barrier();

    // Shuffle
    sem_wait(trdContext->shuffleSemaphore);

    // if shuffle phase isn't over yet - shuffle.
    if (!(*trdContext->isShuffled)) // isShuffled is initialized to false.
    {
        //switch faze to shuffle
        int err = pthread_mutex_lock(trdContext->job->switchFazeMutex);
        if (err) printErr("Failed to lock mutex");
        trdContext->job->stage = SHUFFLE_STAGE;
        *trdContext->pairsProcessed = 0;
        *trdContext->idxCounter = 0;
        err = pthread_mutex_unlock(trdContext->job->switchFazeMutex);
        if (err) printErr("Failed to lock mutex");

        // do shuffle
        shuffle(trdContext->job->victor, trdContext->job->threadContexts);
        *trdContext->isShuffled = true;

        // switch faze to reduce.
        err = pthread_mutex_lock(trdContext->job->switchFazeMutex);
        if (err) printErr("Failed to lock mutex");
        trdContext->job->stage = REDUCE_STAGE;
        *trdContext->pairsProcessed = 0;
        *trdContext->idxCounter = 0;
        err = pthread_mutex_unlock(trdContext->job->switchFazeMutex);
        if (err) printErr("Failed to lock mutex");
    }
    sem_post(trdContext->shuffleSemaphore);

    // 2nd LOOP
    // Get intermediate pairs , reduce & insert to outputVec
    while (*(trdContext->idxCounter) < trdContext->job->victor.size())
    {
        long unsigned int idx = (*(trdContext->idxCounter))++;
        if (idx >= trdContext->job->victor.size()) break;
        trdContext->client->reduce(&trdContext->job->victor.at(idx), trdContext);
        (*trdContext->pairsProcessed)++;
    }
    return nullptr;
}

void printErr(const std::string& str)
{
    std::cerr << "system error: " << str << std::endl;
    exit(1);
}

JobContext * initJob(const InputVec &inputVec, OutputVec *outputVec,
                     std::atomic<long unsigned int>* pairsProcessed,
                     std::atomic<long unsigned int>* totalMappedPairs,
                     std::atomic<bool>* isWaitFor, pthread_mutex_t* waitForMutex, pthread_mutex_t* switchFazeMutex)
{
    auto *job = new(std::nothrow) JobContext();
    if (!job)
    {
        printErr("Failed to allocate memory for the job.");
    }
    job->inputVec = inputVec;
    job->outputVec = outputVec;
    job->stage = UNDEFINED_STAGE;
    job->pairsProcessed = pairsProcessed;
    job->totalMappedPairs = totalMappedPairs;
    job->isWaitedFor = isWaitFor;
    job->waitForMutex = waitForMutex;
    job->switchFazeMutex = switchFazeMutex;
    return job;
}

void initTrdContext(Barrier *barrier,
                    const MapReduceClient &client,
                    JobContext* job,
                    std::atomic<long unsigned int> * idxCounter,
                    std::atomic<long unsigned int> * totalMappedPairs,
                    sem_t* shuffleSemaphore,
                    bool* shufflePointer,
                    pthread_mutex_t* pushBackMutex,
                    std::atomic<long unsigned int>* pairsProcessed)
{
    auto* trd = new (std::nothrow)pthread_t ();
    auto * trdContext = new(std::nothrow) ThreadContext();
    if (!trd || !trdContext)
    {
        printErr("Failed to allocate memory for the job.");
    }
    trdContext->idxCounter = idxCounter;
    trdContext->totalMappedPairs = totalMappedPairs;
    trdContext->barrier = barrier;
    trdContext->client = &client;
    trdContext->shuffleSemaphore = shuffleSemaphore;
    trdContext->isShuffled = shufflePointer;
    trdContext->job = job;
    trdContext->pushBackMutex = pushBackMutex;
    trdContext->pairsProcessed = pairsProcessed;
    int err = pthread_create(trd, nullptr, mapReduce, trdContext);
    if (err) printErr("Failed to create thread");
    job->threadsVec.push_back(trd);
    job->threadContexts.push_back(trdContext);

}

IntermediatePair findMax(std::vector<ThreadContext *>& threadContexts)
{
    IntermediatePair toRet;
    toRet.first = nullptr;
    toRet.second = nullptr;

    for (ThreadContext* thread : threadContexts)
    {
        if (!(thread->intermediateVec.empty()))
        {
            if (!toRet.first)
            {
                toRet = thread->intermediateVec.back();
            } else
            {
                toRet = (*toRet.first < *thread->intermediateVec.back().first) ?
                        thread->intermediateVec.back(): toRet;
            }
        }
    }
    return toRet;
}

IntermediateVec extractMax(IntermediatePair pair, std::vector<ThreadContext *>& threadContexts)
{
    IntermediateVec toReturn;
    for (ThreadContext* thread: threadContexts)
    {
        // check if the intermediate vector contains the pairs' key
        while (!(thread->intermediateVec.empty()) &&
        !(compareIntermediatePairs(pair,thread->intermediateVec.back()) ||
                compareIntermediatePairs(thread->intermediateVec.back(), pair)))
        {
            toReturn.push_back(thread->intermediateVec.back());
            (*thread->idxCounter)++;
            (*thread->pairsProcessed)++;
            thread->intermediateVec.pop_back();
        }
    }
    return toReturn;
}

void shuffle (std::vector<IntermediateVec>& victor,
              std::vector<ThreadContext *>& threadContexts)
{
    IntermediatePair curr = findMax(threadContexts);
    while (curr.first && curr.second)
    {
        victor.push_back(extractMax(curr, threadContexts));
        curr = findMax(threadContexts);
    }
}

void freeContextResources(ThreadContext* context)
{
    delete context->idxCounter;
    delete context->totalMappedPairs;
    delete context->pairsProcessed;
    delete context->barrier;
    delete context->shuffleSemaphore;
    delete context->isShuffled;
    delete context->pushBackMutex;

}