package promise

import (
	"log"
)

// Promise is a used to Sync async methods
type Promise interface {
	Then(success func(interface{}) (interface{}, error)) Promise
	Catch(err func(error))
	ThenOrCatch(success func(interface{}) (interface{}, error), err func(error)) Promise
}

// SimplePromise is a implementation of Promise
type SimplePromise struct {
	successChannel chan interface{}
	failureChannel chan error
}

// FromFunc creates a promise from function
func FromFunc(function func() (interface{}, error)) Promise {
	promise := new(SimplePromise)
	promise.successChannel = make(chan interface{}, 1)
	promise.failureChannel = make(chan error, 1)
	go func() {
		obj, err := function()
		if err == nil {
			promise.successChannel <- obj
		} else {
			promise.failureChannel <- err
		}
	}()
	return promise
}

// From creates promise from object
func From(obj interface{}) *SimplePromise {
	promise := new(SimplePromise)
	promise.successChannel = make(chan interface{}, 1)
	promise.successChannel <- obj
	return promise
}

// Then is sa imple then implemtaion
func (promise *SimplePromise) Then(success func(interface{}) (interface{}, error)) Promise {
	result := new(SimplePromise)
	result.successChannel = make(chan interface{}, 1)
	result.failureChannel = make(chan error, 1)
	go func() {
		obj := <-promise.successChannel
		obj, err := success(obj)
		if err == nil {
			result.successChannel <- obj
		} else {
			log.Println(err)
			result.failureChannel <- err
		}
	}()
	return result
}

// Catch is a error handling method
func (promise *SimplePromise) Catch(errFunc func(error)) {
	go func() {
		err := <-promise.failureChannel
		errFunc(err)
	}()
}

// ThenOrCatch is a method that does both then and catch
func (promise *SimplePromise) ThenOrCatch(success func(interface{}) (interface{}, error), errFunc func(error)) Promise {
	result := new(SimplePromise)
	result.successChannel = make(chan interface{}, 1)
	result.failureChannel = make(chan error, 1)
	go func() {
		select {
		case obj := <-promise.successChannel:
			obj, err := success(obj)
			if err == nil {
				result.successChannel <- obj
			} else {
				log.Println(err)
				result.failureChannel <- err
			}
			break
		case err := <-promise.failureChannel:
			result.failureChannel <- err
			errFunc(err)
			break
		}
	}()
	return result
}
