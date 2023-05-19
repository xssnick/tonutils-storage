package api

import "net/http"

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(addr string) error {
	m := http.NewServeMux()
	m.HandleFunc("/api/v1/download", s.handleDownload)
	m.HandleFunc("/api/v1/create", s.handleCreate)
	m.HandleFunc("/api/v1/remove", s.handleRemove)
	m.HandleFunc("/api/v1/stop", s.handleRemove)
	m.HandleFunc("/api/v1/list", s.handleRemove)
	return http.ListenAndServe(addr, m)
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {

}
