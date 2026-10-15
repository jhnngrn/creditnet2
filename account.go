package creditnet

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	sess, _, err := s.authenticate(r)
	if err != nil {
		HttpError(w, http.StatusUnauthorized, err.Error())
		return
	}
	req, ok := readAPIRequest(w, r)
	if !ok {
		return
	}
	ls := s.getLinks(sess.user)
	if ls == nil {
		HttpError(w, http.StatusInternalServerError, "no node")
		return
	}

	ls.Lock()
	result, appErr := ls.HandleAccount(req.Method, req.Payload)
	if appErr == nil {
		ls.Commit()
	}
	ls.Unlock()
	if appErr != nil {
		ApiError(w, appErr.Error())
		return
	}
	if result == nil {
		result = json.RawMessage("true")
	}
	ApiOK(w, result)
}
