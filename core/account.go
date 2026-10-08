package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"creditnet2/link"
)

var ErrUnknownMethod = errors.New("unknown method")

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

	var m struct {
		Peer    string `json:"peer"`
		Enabled bool   `json:"enabled"`
	}

	n.Lock()

	var result json.RawMessage
	var appErr error
	switch req.Method {
	case "add_link":
		if json.Unmarshal(req.Payload, &m) != nil {
			appErr = fmt.Errorf("bad request")
		} else if appErr = n.AddLink(m.Peer); appErr == nil {
			result = link.Enc("ok")
		}
	case "remove_link":
		if json.Unmarshal(req.Payload, &m) != nil {
			appErr = fmt.Errorf("bad request")
		} else if appErr = n.RemoveLink(m.Peer); appErr == nil {
			result = link.Enc("ok")
		}
	case "accept_link":
		if json.Unmarshal(req.Payload, &m) != nil {
			appErr = fmt.Errorf("bad request")
		} else if appErr = n.AcceptLink(m.Peer); appErr == nil {
			result = link.Enc("ok")
		}
	case "reject_link":
		if json.Unmarshal(req.Payload, &m) != nil {
			appErr = fmt.Errorf("bad request")
		} else if appErr = n.RejectLink(m.Peer); appErr == nil {
			result = link.Enc("ok")
		}
	case "set_link_requests":
		if json.Unmarshal(req.Payload, &m) != nil {
			appErr = fmt.Errorf("bad request")
		} else {
			n.SetLinkRequests(m.Enabled)
			result = link.Enc("ok")
		}
	case "status":
		result = n.Status()
	default:
		appErr = ErrUnknownMethod
	}

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
