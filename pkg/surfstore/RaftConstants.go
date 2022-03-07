package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var ERR_LOG_INCONSISTENT = fmt.Errorf("Log entries inconsistent")
var ERR_WRONG_TERM = fmt.Errorf("Term is out of date")
var ERR_PREVLOGTERM_MISMATCH = fmt.Errorf("Prev log index or prev log term mismatch")
var ERR_MAJORITY_CRASHED = fmt.Errorf("Majority of the nodes are crashed")
