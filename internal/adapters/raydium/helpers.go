package raydium

// ParseCLMMPoolStatePublic is a public wrapper for parseCLMMPoolState
// to allow external testing
func ParseCLMMPoolStatePublic(raw []byte) (*clmmPoolState, bool) {
	return parseCLMMPoolState(raw)
}

// GetAccountInfoPublic is a public wrapper for getAccountInfo
func (c *RPCClient) GetAccountInfoPublic(account string) ([]byte, error) {
	return c.getAccountInfo(account)
}
