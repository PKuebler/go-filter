package main

type Condition struct {
	field      string
	conditions []Condition
	lte        interface{}
	gte        interface{}
	lt         interface{}
	gt         interface{}
	eq         interface{}
}

func (c Condition) match(obj map[string]interface{}) bool {
	switch c.field {
	case "$and":
		// conditions
		for _, sub := range c.conditions {
			if !sub.match(obj) {
				return false
			}
		}
		return true
	case "$or":
		// conditions
		isSuccess := false
		for _, sub := range c.conditions {
			if sub.match(obj) {
				isSuccess = true
			}
		}
		return isSuccess
	default:
		// convert type
		// lte/gte/lt/gt/eq
		//		fmt.Println(reflections.Fields(obj))
		//		if !obj[c.field] {
		//			return false
		//		}

		field := obj[c.field]

		if field == nil {
			return false
		}

		if c.lte != nil && field.(float64) >= c.lte.(float64) {
			return false
		}
		if c.gte != nil && field.(float64) <= c.gte.(float64) {
			return false
		}
		if c.lt != nil && field.(float64) > c.lt.(float64) {
			return false
		}
		if c.gt != nil && field.(float64) < c.gt.(float64) {
			return false
		}
		if c.eq != nil && field.(string) == c.eq.(string) {
			return false
		}

		return true
	}
}
