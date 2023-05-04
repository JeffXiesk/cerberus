// Copyright 2023

package crypto

import (
	// "bytes"
	// "encoding/gob"
	// "fmt"
	"errors"
	"strconv"

	"github.com/JeffXiesk/cerberus/config"
	logger "github.com/rs/zerolog/log"

	bls "github.com/herumi/bls-eth-go-binary/bls"
)

type BLSPubKey struct {
	pubKey *bls.PublicKey
}

type BLSPrivKey struct {
	privKey *bls.SecretKey
}

type BLSPubKeyShare struct {
	pubKey *bls.PublicKey
}

type BLSPrivKeyShare struct {
	privKey *bls.SecretKey
}

type BLSId struct {
	id *bls.ID
}

func Init() {
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
}

func BLSKeyGeneration(K, N, num int) ([]byte, [][][]byte, [][][]byte, [][][]byte) {
	// Init()
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
	msk := make([]bls.SecretKey, K)
	for i := 0; i < K; i++ {
		msk[i].SetByCSPRNG()
	}

	privShares := make([][]*BLSPrivKeyShare, N)
	privShares_serialized := make([][][]byte, N)
	pubShares := make([][]*BLSPubKeyShare, N)
	pubShares_serialized := make([][][]byte, N)
	ids := make([][]bls.ID, N)
	ids_serialized := make([][][]byte, N*num)
	for i := 0; i < N; i++ {
		privShares[i] = make([]*BLSPrivKeyShare, num)
		privShares_serialized[i] = make([][]byte, num)
		pubShares[i] = make([]*BLSPubKeyShare, num)
		pubShares_serialized[i] = make([][]byte, num)
		ids[i] = make([]bls.ID, num)
		ids_serialized[i] = make([][]byte, num)
	}

	for i := 0; i < N*num; i++ {
		ids[i/num][i%num].SetDecString(strconv.Itoa(i + 1))
		ids_serialized[i/num][i%num] = ids[i/num][i%num].Serialize()
		// fmt.Println(ids[i].GetDecString())
	}

	// share secret key
	for i := 0; i < N*num; i++ {
		privShares[i/num][i%num] = new(BLSPrivKeyShare)
		privShares[i/num][i%num].privKey = new(bls.SecretKey)
		privShares[i/num][i%num].privKey.Set(msk, &ids[i/num][i%num])
		privShares_serialized[i/num][i%num] = privShares[i/num][i%num].privKey.Serialize()
		// fmt.Printf("%v\n", privShares[i].privKey.SerializeToHexStr())
		pubShares[i/num][i%num] = new(BLSPubKeyShare)
		pubShares[i/num][i%num].pubKey = new(bls.PublicKey)
		pubShares[i/num][i%num].pubKey = privShares[i/num][i%num].privKey.GetPublicKey()
		pubShares_serialized[i/num][i%num] = pubShares[i/num][i%num].pubKey.Serialize()
	}

	// get master public key
	pub := msk[0].GetPublicKey()
	pub_serilized := pub.Serialize()

	return pub_serilized, privShares_serialized, pubShares_serialized, ids_serialized
}

func BLSSigShare(privShare *BLSPrivKeyShare, msg []byte) ([]byte, error) {
	return privShare.privKey.SignByte(msg).Serialize(), nil
}

func BLSSigShareVerification(pubKey *BLSPubKeyShare, msg []byte, sigShare_ []byte) error {
	var sigShare bls.Sign
	sigShare.Deserialize(sigShare_)
	if sigShare.VerifyByte(pubKey.pubKey, msg) {
		return nil
	} else {
		return errors.New("BLS SigShare Verification Fail")
	}
}

func BLSRecoverSignature(msg []byte, sigShares [][]byte, keyIds [][]byte, t, n int) ([]byte, error) {
	// recover sig from subSigs[K] and subIds[K]
	var sigRecover bls.Sign
	ids := make([]bls.ID, 0, 0)
	// var set map[int]bool
	set := make(map[int]bool)
	for _, i := range keyIds {
		var id bls.ID
		id.Deserialize(i)
		mark, err := strconv.Atoi(id.GetDecString())
		if err != nil {
			logger.Error().Msg("BLSRecoverSignature error !")
			return []byte{}, nil
		}
		_, found := set[mark/config.Config.PrivKeyCnt]
		if found {
			return []byte{}, nil
		}
		set[mark/config.Config.PrivKeyCnt] = true
		// logger.Debug().Msgf("BLSRecoverSignature: id is %s", id.GetDecString())
		ids = append(ids, id)
	}
	sigs := make([]bls.Sign, 0, 0)
	for _, j := range sigShares {
		var sig bls.Sign
		sig.Deserialize(j)
		sigs = append(sigs, sig)
	}
	sigRecover.Recover(sigs, ids)
	return sigRecover.Serialize(), nil
}

func BLSVerifySingature(pubKey *BLSPubKey, msg []byte, signature []byte) error {
	var sigDeserialize bls.Sign
	sigDeserialize.Deserialize(signature)
	if sigDeserialize.VerifyByte(pubKey.pubKey, msg) {
		return nil
	} else {
		return errors.New("BLS Signature Verification Fail")
	}
}

func BLSPubKeyFromBytes(pub_ []byte) (*BLSPubKey, error) {
	var pub bls.PublicKey
	pub.Deserialize(pub_)
	return &BLSPubKey{pubKey: &pub}, nil
}

func BLSPrivKeyShareFromBytes(privKey_ [][]byte) ([]*BLSPrivKeyShare, error) {
	privKeys := make([]*BLSPrivKeyShare, 0, 0)
	for _, j := range privKey_ {
		var privKey bls.SecretKey
		privKey.Deserialize(j)
		privKeys = append(privKeys, &BLSPrivKeyShare{privKey: &privKey})
	}
	return privKeys, nil
}

func BLSPubKeyShareFromBytes(pubKey_ [][][]byte) ([][]*BLSPubKeyShare, error) {
	pubKeyss := make([][]*BLSPubKeyShare, 0, 0)
	for _, j := range pubKey_ {
		pubKeys := make([]*BLSPubKeyShare, 0, 0)
		for _, k := range j {
			var pubKey bls.PublicKey
			pubKey.Deserialize(k)
			pubKeys = append(pubKeys, &BLSPubKeyShare{pubKey: &pubKey})
		}
		pubKeyss = append(pubKeyss, pubKeys)
	}
	return pubKeyss, nil
}

func BLSIdFromBytes(id_ [][]byte) ([]*BLSId, error) {
	ids := make([]*BLSId, 0, 0)
	for _, j := range id_ {
		var id bls.ID
		id.Deserialize(j)
		ids = append(ids, &BLSId{id: &id})
	}
	return ids, nil
}

func BLSIdToBytes(id *BLSId) []byte {
	return id.id.Serialize()
}

func BLSGetIdDecStringByte(id []byte) string {
	var id_des bls.ID
	id_des.Deserialize(id)
	return id_des.GetDecString()
}

func BLSGetIdDecString(id *BLSId) string {
	return id.id.GetDecString()
}

func BLSSIgnCompare(sig1_, sig2_ []byte) bool {
	var sig1, sig2 bls.Sign
	sig1.Deserialize(sig1_)
	sig2.Deserialize(sig2_)
	return sig1.IsEqual(&sig2)
}
