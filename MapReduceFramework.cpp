//
// Created by cheziunix on 02/06/2021.
//

#include "MapReduceFramework.h"
#include <iostream>
#include <pthread.h>
#include <atomic>
#include <algorithm>

/**
 * A struct to save each MapReduce job progress details.
 */
typedef struct
{
    InputVec inputVec;
    IntermediateVec intermediateVec;
    OutputVec outputVec;
    JobState jobState;
    const MapReduceClient* client;
    std::vector<pthread_t*> threads_vec;
    std::atomic<int>* atomic_idx_counter;
    std::atomic<int>* atomic_pairs_count;
}JobContext;

/**
 * A struct to hold relevant data for each thread.
 */
typedef struct
{
    IntermediateVec intermediateVec;
    std::atomic<int>* atomic_mapped_pairs_counter;
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
    auto * interVec = (ThreadContext *) context;
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    interVec->intermediateVec.push_back(pair);
    (*interVec->atomic_mapped_pairs_counter)++;
}

void emit3(K3 *key, V3 *value, void *context)
{

}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
    auto *job = new(std::nothrow) JobContext();
    if (!job){
        printErr("Failed to allocate memory for the job.");
        // TODO:release memory.
        return nullptr;
    }
    job->inputVec = inputVec;
    job->outputVec = outputVec;
    job->client = &client;
    job->jobState={UNDEFINED_STAGE,0};
    job->atomic_idx_counter = new(std::nothrow) std::atomic<int>;
    if (!job->atomic_idx_counter){
        printErr("Failed to allocate memory for the job.");
        // TODO:release memory.
        return nullptr;
    }
    *(job->atomic_idx_counter) = 0;

    for (int i = 0; i < multiThreadLevel; ++i) {
        auto* trd = new (std::nothrow)pthread_t ();
        if (!trd){
            printErr("Failed to allocate memory for the job.");
            // TODO:release memory.
        }
        pthread_create(trd, nullptr, mapReduce, job);
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
    auto * job = (JobContext *) arg;
    std::atomic<int> num;
    auto * interVec = new(std::nothrow) ThreadContext();
    if (!interVec)
    {
        printErr("failed to allocate memory");
        //TODO:release memory.
        delete interVec;
        return nullptr;
    }
    interVec->atomic_mapped_pairs_counter = &num;

    // 1st LOOP
    // Get pair from input, run map on it & insert to local vector
    while (*(job->atomic_idx_counter) < job->inputVec.size())
    {

        int idx = *(job->atomic_idx_counter)++;
        if (idx >= job->inputVec.size()) break;
        job->client->map(job->inputVec.at(idx).first,
                         job->inputVec.at(idx).second, interVec);
    }
    // Sort local vector
    std::sort((interVec->intermediateVec).begin(), (interVec->intermediateVec).end());
    // Barrier

    // 2nd LOOP
    // Get intermediate pairs , reduce & insert to outputVec

    // Update the stage

    delete interVec;
    return nullptr;
}

void printErr(const std::string& str)
{
    std::cerr << "system error: " << str << std::endl;
}
