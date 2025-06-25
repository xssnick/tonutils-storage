package provider

import (
	"github.com/xssnick/tonutils-go/tlb"
	"math/big"
	"reflect"
	"testing"
)

func bigInt(n int64) *big.Int {
	return big.NewInt(n)
}

// coins is a helper that converts an int64 value into a tlb.Coins value.
func coins(n int64) tlb.Coins {
	return tlb.MustFromNano(bigInt(n), 9)
}

func coinsFromStr(n string) tlb.Coins {
	return tlb.MustFromTON(n)
}

func TestCalculateBestProviderOffer(t *testing.T) {
	tests := []struct {
		name  string
		input *ProviderRates
		want  Offer
	}{
		{
			name: "provider unavailable",
			input: &ProviderRates{
				Available: false,
			},
			want: Offer{
				Span:          900,
				Every:         "15 Minutes",
				RatePerMBNano: bigInt(0),
				PerDayNano:    bigInt(0),
				PerProofNano:  bigInt(0),
			},
		},
		{
			name: "provider available with sufficient storage",
			input: &ProviderRates{
				Available:        true,
				RatePerMBDay:     coins(100),
				MinBounty:        coins(50),
				SpaceAvailableMB: 20,
				MinSpan:          5,
				MaxSpan:          10,
				Size:             10,
			},
			want: Offer{
				Span:          900,
				Every:         "15 Minutes",
				RatePerMBNano: bigInt(503316480),
				PerDayNano:    bigInt(4800),
				PerProofNano:  bigInt(50),
			},
		},
		{
			name: "provider available with limited storage",
			input: &ProviderRates{
				Available:        true,
				RatePerMBDay:     coins(200),
				MinBounty:        coins(100),
				SpaceAvailableMB: 5,
				MinSpan:          3,
				MaxSpan:          10,
				Size:             8,
			},
			want: Offer{
				Span:          900,
				Every:         "15 Minutes",
				RatePerMBNano: bigInt(1258291200),
				PerDayNano:    bigInt(9600),
				PerProofNano:  bigInt(100),
			},
		},
		{
			name: "provider available real case",
			input: &ProviderRates{
				Available:        true,
				RatePerMBDay:     coinsFromStr("0.0001"),
				MinBounty:        coinsFromStr("0.05"),
				SpaceAvailableMB: 9<<30 + 400<<20,
				MinSpan:          86400,
				MaxSpan:          86400 * 30,
				Size:             9<<30 + 400<<20,
			},
			want: Offer{
				Span:          2583648,
				Every:         "29 Days",
				RatePerMBNano: bigInt(174),
				PerDayNano:    bigInt(1673183),
				PerProofNano:  coinsFromStr("0.050033778").Nano(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBestProviderOffer(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				mul := new(big.Int).Mul(got.RatePerMBNano, new(big.Int).SetUint64(tt.input.Size))
				mul = mul.Mul(mul, big.NewInt(int64(got.Span)))
				bounty := new(big.Int).Div(mul, big.NewInt(24*60*60*1024*1024))

				println(tlb.MustFromNano(got.PerProofNano, 9).String())
				println(tlb.MustFromNano(bounty, 9).String())

				t.Errorf("CalculateBestProviderOffer() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
