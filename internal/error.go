package internal

import "errors"

var ErrNotSupported = errors.New("not supported")
var ErrTaskNameRequired = errors.New("msgqueue: Message.TaskName is required")
