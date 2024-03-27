package main

func main() {
	// kv, err := NewKv(false)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// kv.Set(1, []byte{1}, []byte{'c', 'a', 'r', 'l'})
	// res, found, err := kv.Get(1, []byte{1})
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Print(res, found)
	cs := map[int]command{
		1:  &initCmd{p2: 9},
		2:  &openReadCmd{p1: 0, p2: 1},
		3:  &rewindCmd{p1: 0, p2: 8},
		4:  &rowIdCmd{p1: 0, p2: 1},
		5:  &columnCmd{p1: 0, p2: 1, p3: 2},
		6:  &resultRowCmd{p1: 1, p2: 2},
		7:  &nextCmd{p1: 0, p2: 4},
		8:  &haltCmd{},
		9:  &transactionCmd{},
		10: &gotoCmd{p2: 2},
	}
	explain(cs)
}
