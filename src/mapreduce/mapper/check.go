package mapper

func checkParams(params *Params) *Params {
	if params.RetryWithDelay == nil {
		panic("Invalid parameters")
	}

	return params
}
