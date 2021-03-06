package agent

import (
	"fmt"
	"github.com/iddqdeika/kafka-adapter"
	"io/ioutil"
	"kafka-agent/nethelper"
	"net/http"
	"sync"
)

const (
	cfgFileName = "cfg.json"
)

func New() (*Agent, error) {
	config, err := kafkaadapt.LoadJsonConfig(cfgFileName)
	if err != nil {
		return nil, err
	}
	q, err := kafkaadapt.FromConfig(config, kafkaadapt.DefaultLogger)
	if err != nil {
		return nil, err
	}

	return &Agent{
		q:          q,
		writersGot: make(map[string]struct{}),
	}, nil
}

type Agent struct {
	q          *kafkaadapt.Queue
	port       int
	writersGot map[string]struct{}
	m          sync.RWMutex
}

func (a *Agent) Run() error {
	addr, err := nethelper.GetCurrentAddr(8091)
	if err != nil {
		return err
	}
	http.HandleFunc("/sendMsgToKafka", a.SendMsgToKafka)
	http.HandleFunc("/echo", echo)
	fmt.Println("kafka agent started on " + addr)
	return http.ListenAndServe(addr, nil)
}

func (a *Agent) SendMsgToKafka(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte("cant read request body"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	queues, ok := r.URL.Query()["queue"]
	if !ok || len(queues) < 1 {
		w.Write([]byte("queue param must be filled in url"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	queue := queues[0]

	a.m.Lock()
	_, ok = a.writersGot[queue]
	if !ok {
		a.q.WriterRegister(queue)
		a.writersGot[queue] = struct{}{}
	}
	a.m.Unlock()

	err = a.q.Put(queue, data)
	if err != nil {
		w.Write([]byte("kafka adapter err: " + err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte("ok"))
	return
}

func echo(rw http.ResponseWriter, req *http.Request) {
	msgs := req.URL.Query()["msg"]
	if len(msgs) >= 1 {
		rw.Write([]byte(msgs[0]))
	}
	return
}
