package protocol

import "sync"

type syncMessage struct {
	id   string
	data interface{}
}

type syncManager struct {
	fromStream chan syncMessage
	newStream  *sync.Cond
	streams    map[string]chan interface{}
	streamLock sync.Locker
}

var (
	managerInstance syncManager
	once            sync.Once
)

func getSyncManager() *syncManager {
	once.Do(func() {
		managerInstance.newStream = sync.NewCond(&sync.Mutex{})
		managerInstance.streams = make(map[string]chan interface{})
		managerInstance.streamLock = &sync.Mutex{}
		go managerInstance.run()
	})

	return &managerInstance
}

func (m *syncManager) registerSyncStream(id string, toStream chan interface{}) chan syncMessage {
	m.streamLock.Lock()
	defer m.streamLock.Unlock()

	m.streams[id] = toStream

	return m.fromStream
}

func (m *syncManager) run() {
	// TODO: Implement sync manager algorithm here
}
