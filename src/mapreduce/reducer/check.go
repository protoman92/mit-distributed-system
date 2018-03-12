package reducer

func checkParams(params *Params) *Params {
	if params.FileAccessor == nil || params.RetryWithDelay == nil {
		panic("Invalid parameters")
	}

	return params
}
