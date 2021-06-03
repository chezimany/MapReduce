//
// Created by cheziunix on 02/06/2021.
//

#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <iostream>
#include <pthread.h>
#include <atomic>
#include <algorithm>

bool compareIntermediatePairs(IntermediatePair  a, IntermediatePair  b) { return (*a.first < *b.first);}

/**
 * A struct to save each MapReduce job progress details.
 */
typedef struct
{
    InputVec inputVec;
    IntermediateVec intermediateVec;
    OutputVec outputVec;
    JobState jobState;
    std::vector<pthread_t*> threads_vec;
}JobContext;

/**
 * A struct to hold relevant data for each thread.
 */
typedef struct
{
    InputVec inputVec;
    IntermediateVec intermediateVec;
    std::atomic<int>* atomic_idx_counter;
    std::atomic<int>* atomic_pairs_count;
    Barrier* barrier;
    const MapReduceClient* client;

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

void emit2(K2 *key, V2 *value, void *context)
{
    auto * interVec = static_cast<ThreadContext *>(context);
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    interVec->intermediateVec.push_back(pair);
    (*interVec->atomic_pairs_count)++;
}

void emit3(K3 *key, V3 *value, void *context)
{

}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                  OutputVec &outputVec, int multiThreadLevel)
{
    auto *job = new(std::nothrow) JobContext();
    if (!job){
        printErr("Failed to allocate memory for the job.");
        // TODO:release memory.
        return nullptr;
    }
    job->inputVec = inputVec;
    job->outputVec = outputVec;
    job->jobState={UNDEFINED_STAGE,0};

    auto * barrier = new(std::nothrow) Barrier(multiThreadLevel);
    if (!barrier)
    {
        printErr("Failed to allocate memory for the job.");
        // TODO:release memory.
        return nullptr;
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        auto* trd = new (std::nothrow)pthread_t ();
        auto * trdContext = new(std::nothrow) ThreadContext();
        trdContext->atomic_idx_counter = new(std::nothrow) std::atomic<int>;
        if (!trd || !trdContext || !trdContext->atomic_idx_counter){
            printErr("Failed to allocate memory for the job.");
            // TODO:release memory.
            return nullptr;
        }
        *(trdContext->atomic_idx_counter) = 0;
        trdContext->inputVec = inputVec;
        trdContext->barrier = barrier;
        trdContext->client = &client;
        pthread_create(trd, nullptr, mapReduce, trdContext);
        job->threads_vec.push_back(trd);
    }
    return job;
}

void waitForJob(JobHandle job)
{

}

void getJobState(JobHandle job, JobState *state)
{

}

void closeJobHandle(JobHandle job)
{

}

void *mapReduce(void* arg)
{
    auto * trdContext = static_cast<ThreadContext *>(arg);
    // 1st LOOP
    // Get pair from input, run map on it & insert to local vector
    while (*(trdContext->atomic_idx_counter) < trdContext->inputVec.size())
    {

        int idx = *(trdContext->atomic_idx_counter)++;
        if (idx >= trdContext->inputVec.size()) break;
        trdContext->client->map(trdContext->inputVec.at(idx).first,
                                trdContext->inputVec.at(idx).second, trdContext);
    }
    // Sort local vector
    std::sort((trdContext->intermediateVec).begin(),
              (trdContext->intermediateVec).end(),
              compareIntermediatePairs);

    // Shuffle of thread 0

    // Barrier

    // 2nd LOOP
    // Get intermediate pairs , reduce & insert to outputVec

    // Update the stage

    delete trdContext;
    return nullptr;
}

void printErr(const std::string& str)
{
    std::cerr << "system error: " << str << std::endl;
}
