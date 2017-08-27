package queue

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/qiniu/log"
)

const (
	StatusInit int32 = iota
	StatusClosed
)

type memoryQueue struct {
	name              string
	maxQueueLength    int
	writeChan         chan []byte
	readChan          chan []byte
	tmpChan           chan []byte
	depth             int64
	mux               sync.Mutex
	status            int32
	diskQueue         BackendQueue
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
}

func NewMemoryQueue(name string, maxQueueLength int, diskQueue BackendQueue) BackendQueue {
	mq := &memoryQueue{
		name:              name,
		mux:               sync.Mutex{},
		writeChan:         make(chan []byte, maxQueueLength),
		readChan:          make(chan []byte),
		tmpChan:           make(chan []byte, 1),
		maxQueueLength:    maxQueueLength,
		status:            StatusInit,
		diskQueue:         diskQueue,
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
	}
	go mq.ioLoop()
	return mq
}

func (mq *memoryQueue) Name() string {
	return mq.name
}

func (mq *memoryQueue) Put(msg []byte) error {
	if len(mq.writeChan) >= mq.maxQueueLength {
		return errors.New("memory queue is full")
	}
	if mq.status == StatusClosed {
		return errors.New("memory queue is closed")
	}
	mq.mux.Lock()
	defer mq.mux.Unlock()
	if len(mq.writeChan) >= mq.maxQueueLength {
		fmt.Println("writeChan size", len(mq.writeChan))
		return errors.New("memory queue is full")
	}
	if mq.status == StatusClosed {
		return errors.New("memory queue is closed")
	}
	mq.writeChan <- msg
	atomic.AddInt64(&mq.depth, 1)
	return nil
}

func (mq *memoryQueue) ReadChan() <-chan []byte {
	return mq.readChan
}

func (mq *memoryQueue) Close() error {
	err := mq.exit(func() error {
		if mq.diskQueue != nil {
			mq.saveDisk()
			return mq.diskQueue.Close()
		}
		return nil
	})
	return err
}

func (mq *memoryQueue) Delete() error {
	err := mq.exit(func() error {
		if mq.diskQueue != nil {
			return mq.diskQueue.Delete()
		}
		return nil
	})
	return err
}

func (mq *memoryQueue) exit(exitDisk func() error) error {
	mq.mux.Lock()
	defer mq.mux.Unlock()
	mq.status = StatusClosed
	close(mq.exitChan)
	<-mq.exitSyncChan
	err := exitDisk()
	close(mq.writeChan)
	close(mq.readChan)
	return err
}

func (mq *memoryQueue) Depth() int64 {
	mq.mux.Lock()
	defer mq.mux.Unlock()
	if mq.diskQueue != nil {
		return atomic.LoadInt64(&mq.depth) + mq.diskQueue.Depth()
	} else {
		return atomic.LoadInt64(&mq.depth)
	}
}

func (mq *memoryQueue) Empty() (err error) {
	if mq.diskQueue != nil {
		err = mq.diskQueue.Empty()
	}
	mq.mux.Lock()
	defer mq.mux.Unlock()
	mq.emptyWriteChan()
	select {
	case <-mq.readChan:
	default:
	}
	atomic.StoreInt64(&mq.depth, 0)
	return
}

func (mq *memoryQueue) emptyWriteChan() {
	for {
		select {
		case <-mq.writeChan:
		default:
			return
		}
	}
}

func (mq *memoryQueue) saveDisk() {
	for {
		var msg []byte
		select {
		case msg = <-mq.tmpChan:
		case msg = <-mq.writeChan:
		default:
			return
		}
		err := mq.diskQueue.Put(msg)
		if err != nil {
			log.Errorf("ERROR: memoryqueue(%s) failed to saveDisk - %s", mq.name, err)
		}
	}
}

func (mq *memoryQueue) ioLoop() {
	var diskReadChan <-chan []byte
	if mq.diskQueue != nil {
		diskReadChan = mq.diskQueue.ReadChan()
	}
	for {
		var msg []byte
		select {
		case msg = <-diskReadChan:
		case msg = <-mq.writeChan:
		case <-mq.exitChan:
			goto exit
		}
		select {
		case mq.readChan <- msg:
			atomic.AddInt64(&mq.depth, -1)
		case <-mq.exitChan:
			select {
			case mq.tmpChan <- msg:
			default:
				log.Errorf("ERROR: memoryqueue(%s) drop one msg")
			}
			atomic.AddInt64(&mq.depth, -1)
			goto exit
		}
	}

exit:
	log.Infof("MEMORYQUEUE(%s): closing ... ioLoop", mq.name)
	mq.exitSyncChan <- 1
}
