package main

import (
	"strings"
)

// Filter Config
type Filter struct {
	Name    string
	In      FilterConnection
	Out     FilterConnection
	Rules   Condition
	_Parsed []string
	RawOut  bool
}

type FilterConnection struct {
	Topic   string
	Service string
}

func (filter *Filter) parse() {
	filter._Parsed = strings.Split(filter.In.Topic, "/")
}

// Check Filter
func (filter *Filter) matchTopic(msg Message) bool {
	i := 0
	for i < len(msg._Parsed) {
		// topic is longer, no match
		if i >= len(filter._Parsed) {
			return false
		}
		// matched up to here, and now the wildcard says "all others will match"
		if filter._Parsed[i] == "#" {
			return true
		}
		// text does not match, and there wasn't a + to excuse it
		if msg._Parsed[i] != filter._Parsed[i] && filter._Parsed[i] != "+" {
			return false
		}
		i++
	}

	// make finance/stock/ibm/# match finance/stock/ibm
	if i == len(filter._Parsed)-1 && filter._Parsed[len(filter._Parsed)-1] == "#" {
		return true
	}

	if i == len(filter._Parsed) {
		return true
	}

	return false
}

func (filter *Filter) matchService(msg Message) bool {
	if filter.In.Service == msg.Service.getName() {
		return true
	}
	return false
}

func (filter *Filter) matchPaylod(msg Message) bool {
	return filter.Rules.match(msg.Payload)
}
