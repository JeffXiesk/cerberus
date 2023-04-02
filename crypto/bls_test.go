// Copyright 2023

package crypto

import (
	"testing"
	"unsafe"
	// bls "github.com/herumi/bls-eth-go-binary/bls"
)

func init() {
	Init()
}

func TestBLSRecoverSignature(t *testing.T) {
	f := 3
	n := 10
	pub_, privShares_, pubShares_, ids := BLSKeyGeneration(f, n, 5)
	privShares, _ := BLSPrivKeyShareFromBytes(privShares_[0])
	pubShares, _ := BLSPubKeyShareFromBytes(pubShares_)
	pub, _ := BLSPubKeyFromBytes(pub_)

	msg := []byte("Hello World!")

	sigShares := make([][]byte, 0, 0)
	idShares := make([][]byte, 0, 0)
	for i := 0; i < f; i++ {
		sigShare, err := BLSSigShare(privShares[i], msg)
		if err != nil {
			t.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
		}
		sigShares = append(sigShares, sigShare)
		idShares = append(idShares, ids[0][i])
		t.Logf("id is %s", BLSGetIdDecStringByte(ids[0][i]))
		t.Logf("K is %d", i)
		err = BLSSigShareVerification(pubShares[0][i], msg, sigShare)
		if err != nil {
			t.Fatalf("Could not vertify %d: %s", i, err.Error())
		}
		t.Logf("%d", unsafe.Sizeof(sigShare))
	}

	signature, err := BLSRecoverSignature(msg, sigShares, idShares, f, n)
	if err != nil {
		t.Fatalf("Signature recovery failed: %s", err.Error())
	}

	err = BLSVerifySingature(pub, msg, signature)
	if err != nil {
		t.Fatalf("Signature verification failed: %s", err.Error())
	}

	t.Logf("%d", unsafe.Sizeof(signature))
	var fakeQC [24]byte
	// copy(fakeQC[:], "fakeQC")
	t.Logf("%d", unsafe.Sizeof(fakeQC))
}

func TestAllSituationBLSRecoverSignature(t *testing.T) {
	f := 3
	n := 5
	pub_, privShares_, pubShares_, ids := BLSKeyGeneration(f, n, 5)
	privShares, _ := BLSPrivKeyShareFromBytes(privShares_[0])
	pubShares, _ := BLSPubKeyShareFromBytes(pubShares_)
	pub, _ := BLSPubKeyFromBytes(pub_)

	msg := []byte("Hello World!")

	idx := CombinationResult(n, f)

	for i := 0; i < len(idx); i++ {
		sigShares := make([][]byte, 0, 0)
		idShares := make([][]byte, 0, 0)
		k := 0
		idx_num := make([]int, f)
		for j := 0; j < n; j++ {
			if idx[i][j] == 1 {
				sigShare, err := BLSSigShare(privShares[j], msg)
				if err != nil {
					t.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
				}
				sigShares = append(sigShares, sigShare)
				idShares = append(idShares, ids[0][j])
				// t.Logf("%s", BLSGetIdDecString(ids[j]))
				idx_num[k] = j
				k++
				err = BLSSigShareVerification(pubShares[0][j], msg, sigShare)
				if err != nil {
					t.Fatalf("Could not vertify %d: %s", i, err.Error())
				}
			}
		}

		signature, err := BLSRecoverSignature(msg, sigShares, idShares, f, n)
		if err != nil {
			t.Fatalf("Signature recovery failed: %s, idx is %v", err.Error(), idx_num)
		}

		err = BLSVerifySingature(pub, msg, signature)
		if err != nil {
			t.Fatalf("Signature verification failed: %s, idx is %v", err.Error(), idx_num)
		}
	}
}

// Find all combination result
func CombinationResult(n int, m int) [][]int {
	if m < 1 || m > n {
		// fmt.Println("Illegal argument. Param m must between 1 and len(nums).")
		return [][]int{}
	}

	result := make([][]int, 0, mathCombination(n, m))
	indexs := make([]int, n)
	for i := 0; i < n; i++ {
		if i < m {
			indexs[i] = 1
		} else {
			indexs[i] = 0
		}
	}

	result = addTo(result, indexs)
	for {
		find := false
		for i := 0; i < n-1; i++ {
			if indexs[i] == 1 && indexs[i+1] == 0 {
				find = true
				indexs[i], indexs[i+1] = 0, 1
				if i > 1 {
					moveOneToLeft(indexs[:i])
				}
				result = addTo(result, indexs)
				break
			}
		}
		if !find {
			break
		}
	}

	return result
}

func addTo(arr [][]int, ele []int) [][]int {
	newEle := make([]int, len(ele))
	copy(newEle, ele)
	arr = append(arr, newEle)
	return arr
}

func moveOneToLeft(leftNums []int) {
	sum := 0
	for i := 0; i < len(leftNums); i++ {
		if leftNums[i] == 1 {
			sum++
		}
	}

	for i := 0; i < len(leftNums); i++ {
		if i < sum {
			leftNums[i] = 1
		} else {
			leftNums[i] = 0
		}
	}
}

func findNumsByIndexs(nums []int, indexs [][]int) [][]int {
	if len(indexs) == 0 {
		return [][]int{}
	}
	result := make([][]int, len(indexs))
	for i, v := range indexs {
		line := make([]int, 0)
		for j, v2 := range v {
			if v2 == 1 {
				line = append(line, nums[j])
			}
		}
		result[i] = line
	}
	return result
}

func mathCombination(n int, m int) int {
	return factorial(n) / (factorial(n-m) * factorial(m))
}

func factorial(n int) int {
	result := 1
	for i := 2; i <= n; i++ {
		result *= i
	}
	return result
}
