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
	n := s.getNode(sess.user)
	if n == nil {
		HttpError(w, http.StatusInternalServerError, "no node")
		return
	}

	n.Lock()
	result, appErr := n.HandleAccount(req.Method, req.Payload)
	if appErr == nil {
		if err := n.Commit(); err != nil {
			n.Unlock()
			HttpError(w, http.StatusInternalServerError, "persist failed")
			return
		}
	}
	n.Unlock()
	if appErr != nil {
		ApiError(w, appErr.Error())
		return
	}
	if result == nil {
		result = json.RawMessage("true")
	}
	ApiOK(w, result)
}
