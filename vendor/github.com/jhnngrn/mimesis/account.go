package mimesis

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	sess, _, err := s.authenticate(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, err.Error())
		return
	}
	req, ok := readAPIRequest(w, r)
	if !ok {
		return
	}
	ls := s.getLinks(sess.user)
	if ls == nil {
		httpError(w, http.StatusInternalServerError, "no node")
		return
	}

	result, appErr := ls.HandleAccount(req.Method, req.Payload)
	if appErr != nil {
		apiError(w, appErr.Error())
		return
	}
	if result == nil {
		result = json.RawMessage("true")
	}
	apiOK(w, result)
}
