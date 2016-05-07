package mapreduce

import "container/list"
import "fmt"
//import "log"
import "time"


type WorkerInfo struct {
	address string
	// You can add definitions here.
	currentJobNumber int
	busy bool
	failed bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// Hand out map and reduce jobs to workers	
// Tell the workers the name of the original input file (mr.file) and job number
// worker knows from which file to read input and to which file to write output
// Each worker sends a Register RPC to the master when it starts. This is available on mr.registerChannel. Process new worker registrations by reading from channel.
// Modify the MapReduce struct to keep track of any additional state (e.g., the set of available workers), and initialize this additional state in the InitMapReduce() function.
// Return only when all jobs have finished
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	sendList := list.New()		// list of jobs that need to be dispatched
	jobList := list.New()		// list of jobs that are waiting to finish
	doneChan := make(chan string)	// dispatcher thread signals on this channel when worker finishes job successfully
	failChan := make(chan struct {jobNumber int; worker string})	// dispatched thread signals here when worker fails to process request
	
	
	// Add all map jobs to lists
	for i := 0; i < mr.nMap; i++ {
		sendList.PushBack(i)
		jobList.PushBack(i)
	}
	
	// Dispatch all map jobs and wait for them to finish
	e := sendList.Front()
	for jobList.Len() > 0 {
		// dispatch jobs if any are waiting
		if e != nil {
			if (mr.SendJob(mr.file, Map, e.Value.(int), mr.nReduce, doneChan, failChan) == true) {
				p := e
				e = e.Next()		// move to next job 
				sendList.Remove(p)	// and remove current job from list only if current job successfully sent
			}
		}	
		
		select {
		case worker := <- mr.registerChannel:	// process new worker registrations
			mr.Workers[worker] = &WorkerInfo{worker, -1, false, false}
			fmt.Printf("Registered worker %v\n", mr.Workers[worker])
			
		case worker := <- doneChan:				// take finished jobs off the jobList and mark the worker as free
			mr.Workers[worker].busy = false
			jobList.Remove(FindListElement(jobList, mr.Workers[worker].currentJobNumber))
			mr.Workers[worker].currentJobNumber = -1
			
		case failure := <- failChan:			// if any job fails, re-add the job to the sendList and mark the worker as failed 
			sendList.PushBack(failure.jobNumber)
			mr.Workers[failure.worker].failed = true
			
		}
		
	}
	
	sendList.Init()	// clear the lists
	jobList.Init()
	
	// Add all reduce jobs to the lists
	for i := 0; i < mr.nReduce; i++ {
		sendList.PushBack(i)
		jobList.PushBack(i)
	}
	
	// Dispatch all reduce jobs and wait for them to finish
	e = sendList.Front()
	for jobList.Len() > 0 {
		// dispatch jobs if any are waiting
		if e != nil {
			if (mr.SendJob(mr.file, Reduce, e.Value.(int), mr.nMap, doneChan, failChan) == true) {
				p := e
				e = e.Next()		// move to next job 
				sendList.Remove(p)	// and remove current job from list only if current job successfully sent
			}
		}	
		
		select {
		case worker := <- mr.registerChannel:	// process new worker registrations
			mr.Workers[worker] = &WorkerInfo{worker, -1, false, false}
			fmt.Printf("Registered worker %v\n", mr.Workers[worker])
			
		case worker := <- doneChan:				// take finished jobs off the jobList and mark the worker as free
			mr.Workers[worker].busy = false
			jobList.Remove(FindListElement(jobList, mr.Workers[worker].currentJobNumber))
			mr.Workers[worker].currentJobNumber = -1
			
		case failure := <- failChan:			// if any job fails, re-add the job to the sendList and mark the worker as failed 
			sendList.PushBack(failure.jobNumber)
			mr.Workers[failure.worker].failed = true
		}
		
	}
	
	return mr.KillWorkers()		// kill the workers and return
}

// Dispatch a job to the next free worker
// Return true if successfully dispatched
func (mr *MapReduce) SendJob(file string, operation JobType, jobNumber int, numOtherPhase int, doneChan chan string, failChan chan struct {jobNumber int; worker string}) bool {
	arg := &DoJobArgs{file, operation, jobNumber, numOtherPhase}	// package the arguments for RPC
	for worker := range mr.Workers {								// check all workers to see if any free
		if (mr.Workers[worker].busy == false && mr.Workers[worker].failed == false) {
			mr.Workers[worker].busy = true							// this is still on master thread so doesn't have to be locked
			mr.Workers[worker].currentJobNumber = jobNumber
			
			// dispatch the job on a separate thread so that master doesn't have to wait for job completion
			go func(worker string, arg *DoJobArgs, doneChan chan string, failChan chan struct {jobNumber int; worker string}) {
				res := &DoJobReply{false}
				ok := call(worker, "Worker.DoJob", arg, res)	// Send the RPC to worker and wait for reply (which will come only when worker finishes job)
				
				if (!ok || !res.OK) {							// If job not successfully finished or RPC failed
					fmt.Printf("Failed to process %v job %d on worker %v\n", arg.Operation, arg.JobNumber, worker)
					failChan <- struct {jobNumber int; worker string}{arg.JobNumber, worker}	// Signal that worker and job have failed
					return
				}
				
				doneChan <- worker	// else signal that job completed successfully
			}(worker, arg, doneChan, failChan)
			return true
		}
	}
	time.Sleep(100 * time.Millisecond)		// If no worker is free, wait for 100 ms
	return false
}

// find the first occurrence of an element in a list
func FindListElement(l *list.List, v interface{}) *list.Element {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == v {
			return e
		}
	}
	
	return nil
}
