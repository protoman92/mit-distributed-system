package fileaccess

func checkParams(params *Params) *Params {
	if params.AccessMethod == "" ||
		params.Network == "" ||
		params.RPCCaller == nil {
		panic("Invalid setup")
	}

	return params
}
