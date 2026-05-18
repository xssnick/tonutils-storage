package storage

import (
	"context"
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const merkleHashBits = 256

type merkleHash [merkleHashBits / 8]byte

func buildMerkleTree(ctx context.Context, pieceHashes []merkleHash, _ int) (*cell.Cell, error) {
	if len(pieceHashes) == 0 {
		return nil, fmt.Errorf("empty merkle tree")
	}

	_, paddedLeaves, err := merkleTreeShape(len(pieceHashes))
	if err != nil {
		return nil, err
	}

	cells := make([]*cell.Cell, paddedLeaves)
	for i := range cells {
		if i&4095 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
		}

		var hash merkleHash
		if i < len(pieceHashes) {
			hash = pieceHashes[i]
		}
		cells[i] = cell.BeginCell().MustStoreSlice(hash[:], merkleHashBits).EndCell()
	}

	for len(cells) > 1 {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		parents := make([]*cell.Cell, len(cells)/2)
		for i := range parents {
			parents[i] = cell.BeginCell().
				MustStoreRef(cells[i*2]).
				MustStoreRef(cells[i*2+1]).
				EndCell()
		}
		cells = parents
	}

	return cells[0], ctx.Err()
}

func (t *Torrent) fastProof(root *cell.Cell, piece, piecesNum uint32) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("merkle root is nil")
	}
	if piece >= piecesNum {
		return nil, fmt.Errorf("piece is out of range %d/%d", piece, piecesNum)
	}

	depth, _, err := merkleTreeShape(int(piecesNum))
	if err != nil {
		return nil, err
	}

	builder := cell.NewMerkleProofBuilder(root)
	current := builder.Root()
	if _, err = current.BeginParse(); err != nil {
		return nil, err
	}
	for bit := int(depth) - 1; bit >= 0; bit-- {
		ref := int((piece >> uint(bit)) & 1)
		current, err = current.PeekRef(ref)
		if err != nil {
			return nil, err
		}
		if _, err = current.BeginParse(); err != nil {
			return nil, err
		}
	}

	return builder.CreateProof()
}

func merkleTreeShape(leaves int) (uint16, int, error) {
	if leaves <= 0 {
		return 0, 0, fmt.Errorf("empty merkle tree")
	}

	var depth uint16
	padded := 1
	for padded < leaves {
		if padded > int(^uint(0)>>1)/2 {
			return 0, 0, fmt.Errorf("merkle tree is too large")
		}
		padded <<= 1
		depth++
	}
	return depth, padded, nil
}
