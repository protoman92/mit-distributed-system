package executor

import (
	"fmt"
)

// Executor represents a MapReduce executor.
type Executor interface {
	InputReceiptChannel() chan<- []byte
	DoneChannel() <-chan interface{}
}

// Params represents the necessary parameters to build a Executor.
type Params struct{}

type executor struct {
	*Params
	doneCh  chan interface{}
	inputCh chan []byte
}

func (e *executor) InputReceiptChannel() chan<- []byte {
	return e.inputCh
}

func (e *executor) DoneChannel() <-chan interface{} {
	return e.doneCh
}

func (e *executor) loopWork() {
	for {
		select {
		case input := <-e.inputCh:
			fmt.Println(string(input))
		}
	}
}

// NewExecutor returns a new Executor.
func NewExecutor(params Params) Executor {
	executor := &executor{
		Params:  &params,
		doneCh:  make(chan interface{}, 1),
		inputCh: make(chan []byte, 1),
	}

	go executor.loopWork()
	return executor
}

// // import "os/exec"

// // A simple mapreduce library with a sequential implementation.
// //
// // The application provides an input file f, a Map and Reduce function,
// // and the number of nMap and nReduce tasks.
// //
// // Split() splits the file f in nMap input files:
// //    f-0, f-1, ..., f-<nMap-1>
// // one for each Map job.
// //
// // DoMap() runs Map on each map file and produces nReduce files for a
// // map file.  Thus, there will be nMap x nReduce files after all map
// // jobs are done:
// //    f-0-0, ..., f-0-0, f-0-<nReduce-1>, ...,
// //    f-<nMap-1>-0, ... f-<nMap-1>-<nReduce-1>.
// //
// // DoReduce() collects <nReduce> reduce files from each map (f-*-<reduce>),
// // and runs Reduce on those files.  This produces <nReduce> result files,
// // which Merge() merges into a single output.

// // Debug toggles debugging capabilities.
// const Debug = 0

// // DPrintf format-prints based on whether Debugging mode is active.
// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		n, err = fmt.Printf(format, a...)
// 	}

// 	return
// }

// // KeyValue represents a tuple of key-value.
// type KeyValue struct {
// 	Key   string
// 	Value string
// }

// // MapReduce represents a MapReduce process.
// type MapReduce interface {
// 	DoneChannel() <-chan bool
// }

// type mapReduce struct {
// 	Params
// 	registerChannel chan string
// 	alive           bool
// 	doneCh          chan bool
// 	l               net.Listener
// 	stats           *list.List

// 	// // Map of registered workers that you need to keep up to date
// 	// Workers map[string]*WorkerInfo

// 	// add any additional state here
// }

// // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Helper functions.
// // initMapReduce initializes a MapReduce process.
// func initMapReduce(params Params) *mapReduce {
// 	mr := &mapReduce{
// 		Params:          params,
// 		alive:           true,
// 		registerChannel: make(chan string, 0),
// 		doneCh:          make(chan bool, 0),
// 	}

// 	return mr
// }

// // // MakeMapReduce initalizes a MapReduce process and kickstart it.
// // func MakeMapReduce(params Params) MapReduce {
// // 	mr := initMapReduce(params)
// // 	mr.StartRegistrationServer()
// // 	go mr.Run()
// // 	return mr
// // }

// func hash(s string) uint32 {
// 	h := fnv.New32a()
// 	h.Write([]byte(s))
// 	return h.Sum32()
// }

// // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> MapReduce methods.
// // Register registers a Worker.
// func (mr *mapReduce) Register(args *RegisterArgs, res *RegisterReply) error {
// 	DPrintf("Register: worker %s\n", args.Worker)
// 	mr.registerChannel <- args.Worker
// 	res.OK = true
// 	return nil
// }

// func (mr *mapReduce) DoneChannel() <-chan bool {
// 	return mr.doneCh
// }

// // Shutdown shuts down a MapReduce process.
// func (mr *mapReduce) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
// 	DPrintf("Shutdown: registration server\n")
// 	mr.alive = false
// 	mr.l.Close() // causes the Accept to fail
// 	return nil
// }

// // StartRegistrationServer starts the registration server that accepts workers.
// func (mr *mapReduce) StartRegistrationServer() {
// 	rpcs := rpc.NewServer()
// 	rpcs.Register(mr)
// 	os.Remove(mr.MasterAddress) // only needed for "unix"
// 	l, e := net.Listen("unix", mr.MasterAddress)

// 	if e != nil {
// 		log.Fatal("RegstrationServer", mr.MasterAddress, " error: ", e)
// 	}

// 	mr.l = l

// 	// now that we are listening on the master address, can fork off accepting
// 	// connections to another thread.
// 	go func() {
// 		for mr.alive {
// 			conn, err := mr.l.Accept()

// 			if err == nil {
// 				go func() {
// 					rpcs.ServeConn(conn)
// 					conn.Close()
// 				}()
// 			} else {
// 				DPrintf("RegistrationServer: accept error", err)
// 				break
// 			}
// 		}

// 		DPrintf("RegistrationServer: done\n")
// 	}()
// }

// // Split splits bytes of input file into nMap splits, but only on white space.
// func (mr *mapReduce) Split(fileName string) {
// 	fmt.Printf("Split %s\n", fileName)
// 	infile, err := os.Open(fileName)

// 	if err != nil {
// 		log.Fatal("Split: ", err)
// 	}

// 	defer infile.Close()
// 	fi, err := infile.Stat()

// 	if err != nil {
// 		log.Fatal("Split: ", err)
// 	}

// 	size := fi.Size()
// 	nchunk := size / int64(mr.NMap)
// 	nchunk++
// 	outfile, err := os.Create(MapName(mr.File, 0))

// 	if err != nil {
// 		log.Fatal("Split: ", err)
// 	}

// 	writer := bufio.NewWriter(outfile)
// 	m := 1
// 	i := 0

// 	scanner := bufio.NewScanner(infile)

// 	for scanner.Scan() {
// 		if int64(i) > nchunk*int64(m) {
// 			writer.Flush()
// 			outfile.Close()
// 			outfile, err = os.Create(MapName(mr.File, m))
// 			writer = bufio.NewWriter(outfile)
// 			m++
// 		}

// 		line := scanner.Text() + "\n"
// 		writer.WriteString(line)
// 		i += len(line)
// 	}

// 	writer.Flush()
// 	outfile.Close()
// }

// // Merge merges the results of the reduce jobs
// // XXX use merge sort
// func (mr *mapReduce) Merge() {
// 	DPrintf("Merge phase")
// 	kvs := make(map[string]string)

// 	for i := 0; i < mr.NReduce; i++ {
// 		p := MergeName(mr.File, i)
// 		fmt.Printf("Merge: read %s\n", p)
// 		file, err := os.Open(p)

// 		if err != nil {
// 			log.Fatal("Merge: ", err)
// 		}

// 		dec := json.NewDecoder(file)

// 		for {
// 			var kv KeyValue
// 			err = dec.Decode(&kv)

// 			if err != nil {
// 				break
// 			}

// 			kvs[kv.Key] = kv.Value
// 		}

// 		file.Close()
// 	}

// 	var keys []string

// 	for k := range kvs {
// 		keys = append(keys, k)
// 	}

// 	sort.Strings(keys)
// 	file, err := os.Create("mrtmp." + mr.File)

// 	if err != nil {
// 		log.Fatal("Merge: create ", err)
// 	}

// 	w := bufio.NewWriter(file)

// 	for _, k := range keys {
// 		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
// 	}

// 	w.Flush()
// 	file.Close()
// }

// // RemoveFile removes a file.
// func RemoveFile(n string) {
// 	err := os.Remove(n)

// 	if err != nil {
// 		log.Fatal("CleanupFiles ", err)
// 	}
// }

// // CleanupFiles cleans up files.
// func (mr *mapReduce) CleanupFiles() {
// 	for i := 0; i < mr.NMap; i++ {
// 		RemoveFile(MapName(mr.File, i))

// 		for j := 0; j < mr.NReduce; j++ {
// 			RemoveFile(ReduceName(mr.File, i, j))
// 		}
// 	}

// 	for i := 0; i < mr.NReduce; i++ {
// 		RemoveFile(MergeName(mr.File, i))
// 	}

// 	RemoveFile("mrtmp." + mr.File)
// }

// // RunSingle run jobs sequentially.
// func RunSingle(params Params, Map func(string) *list.List, Reduce func(string, *list.List) string) {
// 	mr := initMapReduce(params)
// 	mr.Split(mr.File)

// 	for i := 0; i < mr.NMap; i++ {
// 		DoMap(i, mr.File, mr.NReduce, Map)
// 	}

// 	for i := 0; i < mr.NReduce; i++ {
// 		DoReduce(i, mr.File, mr.NMap, Reduce)
// 	}

// 	mr.Merge()
// }

// // CleanupRegistration cleans up all registrations.
// func (mr *mapReduce) CleanupRegistration() {
// 	args := &ShutdownArgs{}
// 	var reply ShutdownReply
// 	ok := call(mr.MasterAddress, "MapReduce.Shutdown", args, &reply)

// 	if ok == false {
// 		fmt.Printf("Cleanup: RPC %s error\n", mr.MasterAddress)
// 	}

// 	DPrintf("CleanupRegistration: done\n")
// }

// // // Run runs jobs in parallel, assuming a shared file system
// // func (mr *mapReduce) Run() {
// // 	fmt.Printf("Run mapreduce job %s %s\n", mr.MasterAddress, mr.File)

// // 	mr.Split(mr.File)
// // 	mr.stats = mr.RunMaster()
// // 	mr.Merge()
// // 	mr.CleanupRegistration()

// // 	fmt.Printf("%s: MapReduce done\n", mr.MasterAddress)

// // 	mr.doneCh <- true
// // }
