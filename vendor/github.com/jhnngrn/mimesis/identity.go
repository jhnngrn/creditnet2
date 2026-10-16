package mimesis

import (
	"fmt"
	"strings"
)

// ParseIdentity parses a user@domain identity into its parts.
// It lowercases the identity before parsing.
func ParseIdentity(id string) (user, domain string, err error) {
	id = strings.ToLower(id)
	i := strings.Index(id, "@")
	if i <= 0 || i == len(id)-1 {
		return "", "", fmt.Errorf("invalid identity %q (want user@domain)", id)
	}
	user, domain = id[:i], id[i+1:]
	if !validUsername(user) {
		return "", "", fmt.Errorf("invalid username %q", user)
	}
	return user, domain, nil
}

// validUsername returns true if u is a valid username component.
func validUsername(u string) bool {
	if len(u) == 0 || len(u) > 64 {
		return false
	}
	for _, c := range u {
		if !(c == '_' || c == '-' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

// peerDomain extracts the domain part of a peer identity.
func peerDomain(peer string) string {
	_, d, err := ParseIdentity(peer)
	if err != nil {
		return ""
	}
	return d
}

// identityInDomain reports whether the identity belongs to the given domain.
func identityInDomain(identity, domain string) bool {
	_, d, err := ParseIdentity(identity)
	return err == nil && d == domain
}
