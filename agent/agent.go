package agent

import (
	"fmt"
	"github.com/iddqdeika/kafka-adapter"
	"io/ioutil"
	"kafka-agent/nethelper"
	"net/http"
)

func New() (*Agent, error){
	config, err := kafkaadapt.LoadJsonConfig("cfg.json")
	if err != nil{
		return nil, err
	}
	q, err := kafkaadapt.FromConfig(config, kafkaadapt.DefaultLogger)
	if err != nil{
		return nil, err
	}
	return &Agent{q:q}, nil
}


type Agent struct {
	q	*kafkaadapt.Queue

}

func (a *Agent) Run() error{
	addr, err := nethelper.GetCurrentAddr(8091)
	if err != nil{
		return err
	}
	http.HandleFunc("/sendMsgToKafka", a.SendMsgToKafka)
	fmt.Println("kafka agent started on " + addr)
	return http.ListenAndServe(addr, nil)
}

func (a *Agent) SendMsgToKafka(w http.ResponseWriter, r *http.Request){
	data, err := ioutil.ReadAll(r.Body)
	if err != nil{
		w.Write([]byte("cant read request body"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	queues, ok := r.URL.Query()["queue"]
	if !ok || len(queues)<1{
		w.Write([]byte("queue param must be filled in url"))
		w.WriteHeader(http.StatusBadRequest)
	}
	queue := queues[0]
	a.q.WriterRegister(queue)
	err = a.q.Put(queue, data)
	if err != nil{
		w.Write([]byte("kafka adapter err: " + err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte("ok"))
	return
}
