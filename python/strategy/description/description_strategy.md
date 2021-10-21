# Strategy
## Commodity 전략
- Commodity 전략은 공통적으로 WEATHER GROUP, NOT WEATHER GROUP으로 나누어서 LONG SHORT 자산을 선택(특정 경우에는 주석 처리되어 있는 경우도 있음)


### CPM - Commodity Price Momentum 전략
1. Momentum 기준으로 LONG SHORT
2. Long term Momentum 과  Short term Momentum을 1:1 배율로 조합
3. Long term Momentum 과 Short term Momentum에 대한 설명은 다음과 같음
    - Long term Momentum
        - 기준: 52주 상승률
        - CS: 상대 모멘텀 * (1/3) + 절대 모멘텀 * (1/3)
        - TS: 상대 모멘텀 * (1/3) + 절대 모멘텀 * (1/3)
    - Short term Momentum
        - 기준: 17주 상승률
        - CS: 상대 모멘텀(1/3)
        - TS: 상대 모멘텀(1/3)

### CVO - Comoodity Volatility 전략
1. 52주 Skewness를 기준으로 Long Short을 한다.
    - Skewness 정의: sum((x-np.mean(x)) ** 3/np.var(x) ** (3/2))/len(x)
    - Skewness가 작은 것을 Long (상위 30%)
    - Skewness * 큰 것을 Short 한다. (하위 30%)
- 참고 자료: [https://blog.naver.com/tysinvs/221551085151](https://blog.naver.com/tysinvs/221551085151)


### CVA -  Commodity Value 전략
1. 과거 63일 동안 (하락 변동성 - 상승 변동성)을 기준으로 LONG SHORT
2. (하락 변동성 - 상승 변동성)이 큰 경우 LONG, 작은 경우 SHORT
3. (하락 변동성 - 상승 변동성)이 크다는 것은 많이 빠졌다는 의미로 많이 빠진 것을 사고 많이 오른 것을 파는 것이기 때문에 Value 로 이름 붙임
- 대표님의 요구로 날씨 민감 그룹과 날씨 민감하지 않은 그룹을 나눠서 실제 했었음. -> 나누지 않는 것이 더 나았음.

## EQUITY STRATEGY

### EPM - Equity Price Momentum 전략

- 여러가지 모멘텀 전략이 합쳐져 있는 전략. 전략의 종류는 4가지
    1. Long-Term 모멘텀: TS 전략에만 적용하며 52주-2주 & 2주 Shift로 + 일때 롱
        - 지수 자체 모멘텀 및 수익률 0 기준 up down 모멘텀 2가지 신호 존재
    2. Shot-Term 모멘텀: TS 전략에만 적용하며 13주-2주 & 2주 Shift로 + 일때 롱
        - 지수 자체 모멘텀 및 수익률 0 기준 up down 모멘텀 2가지 신호 존재
    3. Shot-Term Reversal: CS 전략에만 적용하며 1주, 4주 수익률 순위의 역순으로 나쁜 지수를 롱, 좋은 지수를 숏.
    4. EPS 모멘텀과 가격 모멘텀의 비교: CS 전략에만 적용하며 EPS의 1개월 변동률을 1개월 가격 변동률로 뺀 숫자가 높을 수록 롱, 낮을 수록 숏.
- 총 인덱스 갯수는 14개 인데 이중 아시아 관련 4개를 따로 빼서 Cross Sectional을 하고 나머지 10개에 대해서 Cross Sectional을 따로 함. 이유는 걍 지시 사항이였다고.
- 각각의 시그널에 대해서 확인할 필요

### EEM - Equity Earning Momentum 전략

- (현재 달 ERR - 12 개월 평균 ERR)의 6개월 이동평균이 큰 것을 LONG 작은 것을 SHORT

### EQL - Equity Quality
- Equity Quality 전략
    - Signal1: ROA 상승률 높은 것 Long 및 ROA 상승률 낮은 것 Short
    - Signal1: ICR 상승률 높은 것 Long 및 ICR 상승률 낮은 것 Short

### EST - EQUITY ST

- Grow Value가 높은 Equity Long 낮은 Equity Short 
    - sent = growvalue의 3일 변동성 - growthvalue(v) 3일 변동성
    - sent가 큰 것을 LONG sent가 작은 것을 SHORT

### EPE - Equity Price Earning Ratio
- 2개의 Signal 사용
    - Signal1: EPS / INDEX
    - Signal2: EPS / EPS1
- SIGNAL1, SIGNAL2 합이 크면 LONG 낮으면 SHORT


### ELQ - EQUITY LIQUIDITY
- m2gdp strength 높은 것 Long, m2gdp strength 가 낮은 것 Short
    - mg2dp strength: (D+12m) m2gdp / (D+0m) m2gdp
        - Signal 1: m2gdp strength
        - Signal 2: Signal1 / 12month std of m2gdp


## EMERGING STRATEGY

### EMPM - Emerging Price Momentum  전략
1. Momentum 기준으로 Long Short
2. 기준 Signal
- CS_signal : (52주 -2주) price momentum signal  & 상승 비율 * 0.3 + 
              (13주 -2주) price momentum & 상승비율  signal * 0.3  + 
              4주 price momentum의 역수 signal + 
              1주 price momentum의 역수 signal * 0.5
- TS_signal : (52주 -2주) price momentum signal & 상승 비율 * 0.5  + 
              (13주 -2주) price momentum & 상승비율 signal
    - signal 이 높은 것 LONG, 작은 것 SHORT
    
    
### EMPE - Emerging Price Earning Ratio
1. signal : EPS/지수가격 signal + EPS/EPS1 signal * 0.8
    - signal이 높은 것 long, 작은 것 short


### EMDY - Emerging Dividend Yield 전략
- CS_signal : DPS/지수 3일 변화
- TS_signal : DPS/지수 - 배당률
    - signal이 높은 것 long, 작은 것 short

        
    
## INTEREST STRATEGY

### IPM  - Interest Price Momentum  전략
1. Momentum 기준으로 Long short
2. 국채가격의 가격 모멘텀을 이용한 전략. 
3. 기준 SIGNAL: 1달전 48주 수익률 * 1/3 * 100% + 1달 수익률 * 1/3 * 50%  
- Notes. 상승한 날과 하락한 날, 변동성 등을 사용해서 시그널의 안정성 등을 테스트한 코드들이 남아 있음.
   
 
### ISS - Interest rate SeaSonality 전략
- Time Series 전략은 매월 말 24일부터 마지막 영업일 전일까지 Long, 나머지 날에는 소폭 숏하는 전략. Cross Sectional은 97년부터 매년 해당 월의 Sharpe Ratio를 계산해서 높은 종목을 롱, 낮은 종목을 숏.
    - Notes. 위에 전략 뿐 아니라 계절성을 이용한 전략들을 시도한 코드들이 남아 있으나 위에꺼만 사용. 예를 들어 FOMC 날짜를 활용한 전략들도 있긴 했음.
    - Notes2. 근데,, 이거 R에서 시그널이 원래 의도랑 다른거 아닌가?? 밑에꺼랑 숫자가 한달씩 어긋나는데 수익률 곡선은 비슷(이상함)
    
### IEQ - Interest Rate EQUITY 전략
- 각 나라의 지수의 1, 3, 6개월 수익률의 합의 과거 역사적 순위(Expanding window) 가 높으면 Long, 낮으면 Short.
- 이 원래 전략인 듯 한데 그렇게 하면 계속 손실, 반대로 하면 잘 나온다. 
- 근데 R에서 돌려보면 원래대로 하는 것이 더 나은 것처럼,, 나온다. 
- 이게 왜 이런지 지금은 잘 안보임. 나중에 보자


### ICA - Interest Carry 전략
- 10년 - 2년 금리차를 Carry로 Carry 변동성으로 나눠 순위를 매긴 후 매월 리밸런싱 하는 전략
- 높은 것을 LONG 낮은 것을 Short


### ICA2 - Interest Carry 전략
- Carry 정의는 ICA와 같음
- TS는 no position zone 이 없이 그냥 carry가 높으면 1, 낮을 수록 0에 가깝게 포지션을 잡는, 항상 롱인 전략. 
- CS는 현재 Carry와 3개월, 6개월 Carry 차이를 비교해서 크게 차이나는 것을 LONG, 작게 차이나는 것을 SHORT


