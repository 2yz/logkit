package queue

import (
	"testing"

	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryQueue(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueue", 10, nil)
	assert.Equal(t, "TestMemoryQueue", mq.Name())
	ch := mq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}
	wg.Add(1)
	go func() {
		for range puts {
			exp := <-ch
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	for _, v := range puts {
		err := mq.Put([]byte(v))
		if err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, puts, recv)
}

func TestMemoryQueueWithDQ(t *testing.T) {
	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	recv := []string{}

	dq1 := NewDiskQueue(dqName, tmpDir, 100*1024*1024, 0, 100*1024*1024, 2500, 2500, time.Second*2, 10*1024*1024)
	mq1 := NewMemoryQueue("TestMemoryQueue", 10, dq1)
	for _, v := range puts {
		err := mq1.Put([]byte(v))
		if err != nil {
			t.Error(err)
		}
	}
	mq1.Close()

	dq2 := NewDiskQueue(dqName, tmpDir, 100*1024*1024, 0, 100*1024*1024, 2500, 2500, time.Second*2, 10*1024*1024)
	mq2 := NewMemoryQueue("TestMemoryQueue", 10, dq2)
	ch := mq2.ReadChan()
	wg.Add(1)
	go func() {
		for range puts {
			exp := <-ch
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	wg.Wait()
	err = mq2.Close()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, puts, recv)
}

func TestMemoryQueueEmpty(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueueEmpty", 10, nil)
	assert.Equal(t, "TestMemoryQueueEmpty", mq.Name())
	ch := mq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i := 0; i < 3; i++ {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	if err := mq.Empty(); err != nil {
		t.Error(err)
	}
	for i := 3; i < len(puts); i++ {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			t.Error(err)
		}
	}
	recv := []string{}
	wg.Add(1)
	go func() {
		for range puts[3:] {
			exp := <-ch
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	wg.Wait()
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, puts[3:], recv)
}

func TestMemoryQueueFull(t *testing.T) {
	mq := NewMemoryQueue("TestMemoryQueueFull", 5, nil)
	assert.Equal(t, "TestMemoryQueueFull", mq.Name())
	ch := mq.ReadChan()
	var wg sync.WaitGroup
	puts := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i := 0; i < len(puts); {
		err := mq.Put([]byte(puts[i]))
		if err != nil {
			if err := mq.Empty(); err != nil {
				t.Error(err)
			}
		} else {
			i++
		}
	}
	recv := []string{}
	wg.Add(1)
	go func() {
		for range puts[5:] {
			exp := <-ch
			recv = append(recv, string(exp))
		}
		wg.Done()
	}()
	wg.Wait()
	err := mq.Close()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, puts[5:], recv)

}
