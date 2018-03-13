// Package mapreduce is a MapReduce implementation. Steps:
//
// - Master receives a job request of F file paths.
// - Master creates F * M map jobs.
// - Master sends map jobs to workers.
// - Each worker splits the file contents of the received file path into R
// 	 portions, so in total we will have F * M * R files.
// - For each completed map job, master creates 1 corresponding Reduce job.
// - Each worker receiving a Reduce job reads R files and reduce-merge into 1.
//   By now, the total number of output files is F * M.
// - The master receives completion for all Reduce tasks and notify the client.
// - The client reads F * M result files and perform its own summation.
package mapreduce
