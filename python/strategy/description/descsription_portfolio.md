# 1. 공통
- 각각의 RISK PREMIA에 따른 LONG SHORT 전략을 구사
- 전략 개요
    - TIME SERIES(자신 과거와 비교) 를 통한 LONG SHORT
    - CROSS SECTIONAL(다른 자산과 비교)를 통한 LONG SHORT 
- 2가지 변동성 조절 전략 사용
    - TS POSITION VOL CONTROL
    - STRATEGY LEVEL VOL CONTROL
   
   
# 2. TIME SERIES

1. TS POSITION을 REBALANCING DAY에 맞게 ALIGN 

2. 수익률 일별 변동성 계산: (수익률의 260일 ROLLING STD) * np.sqrt(260)

3. 수익률 VOLBAND 변동성 조절
    - 수익률 일별 변동성 차이가 5% 이상 나는 것이 한 자산도 없다면 전일 수익률 변동성으로 입력
    - 수익률 일별 변동성 차이가 5% 이상 나는 것이 한 자산이라도 있다면 수익률 변동성 업데이트
       (즉, 변동성 조절 전략보다 변동성을 더 크게 잡는다. 변동성을 크게 잡음으로써 보수적인 접근을 취함)

4. TS POSITION VOL CONTROL 계산
    - VCTSpos: (2% TARGET Volatility / 수익률 VOLBAND 변동성 조절) * TSRV
        - 자산 Volatility는 선택인 듯

5. 전략 일별 변동성 계산
    - STRATEGY: (2% TARGET Volatility / 수익률 VOLBAND 변동성 조절) * 수익률(RET) * 시그널(TSRV) = 전략 일별 수익률
    - STRATEGY RISK: (1/2) * (260일 STRATEGY ROLLING STD) + (1/2) * (260일 STRATEGY EXPANDING STD)

6. 전략 VOLBAND 변동성 조절
    - 일별 STRATEGY RISK 차이가 5% 이상 나지 않거나 리밸런싱 일이 아니라면 전일 전략으로 유지
    - 일별 STRATEGY RISK 차이가 5% 이상 나고 리밸런싱 일이라면 전략 업데이트 

7. STRATEGY LEVEL VOL CONTROL 계산
    - STATLEV: (2% TARGET Volatility / 전략 VOLBAND 변동성 조절) 
    
8. FINAL POSTIION 
    - TSPOSITION: VCTSpos * STATLEV
    
    
# 3. CROSS SECTIONAL

1. 수익률 일별 변동성 계산: (수익률의 260일 ROLLING STD) * np.sqrt(260)

2. CS STRATEGY TYPE에 따라 CS POSITION 처리
    1. CS STRATEGY TYPE = VOL: 수익률 VOLBAND 변동성 조절
        - 수익률 일별 변동성 차이가 5% 이상 나는 것이 한 자산도 없다면 전일 수익률 변동성으로 입력
        - 수익률 일별 변동성 차이가 5% 이상 나는 것이 한 자산이라도 있다면 수익률 변동성 업데이트
           (즉, 변동성 조절 전략보다 변동성을 더 크게 잡는다. 변동성을 크게 잡음으로써 보수적인 접근을 취함)
           
        - CS POSITION VOL CONTROL 계산
            - CSRV: (2% TARGET Volatility / 수익률 VOLBAND 변동성 조절) * CSRV
        
    2. CS STRATEGY TYPE = NOTIONAL
        - VOL CONTORL 하지 않고 원본 CS POSITION 유지
        - CSRV = CSRV
        
3. CS POSITION을 REBALANCING DAY에 맞게 ALIGN 및 공분산 계산을 통한 전략 일별 변동성 조절
    - CSRV = CSRV * (2% Target Volatility) / (자산별 수익률 COVARINACE)
     
4. 전략 일별 변동성 계산
    - Strategy: CSRV * 수익률(RET) = 전략 일별 수익률
    - StrategyRisk: (260일 STRATEGY ROLLING STD)

5. 전략 VOLBAND 변동성 조절
    - 일별 STRATEGY RISK 차이가 5% 이상 나지 않거나 리밸런싱 일이 아니라면 전일 전략으로 유지
    - 일별 STRATEGY RISK 차이가 5% 이상 나고 리밸런싱 일이라면 전략 업데이트 

6. STRATEGY LEVEL VOL CONTROL 계산
    - STATLEV: (2% TARGET Volatility / 전략 VOLBAND 변동성 조절) 

7. FINAL POSTIION 
    - TSPOSITION: CSRV * STATLEV
    
    
# 4. 기존 R 코드에서 이상한 점
- TARGET VOLATILITY 조정할 때
    - rebalancing day 고려하지 않음 
        - CS Volatility 1) TARGET 2) COVARIANCE
    
 - RISK VOLATILITY 조정할 때
        - TS는 rebalancing day 고려하지 않음 
        - CS는 rebalancing day 고려


# 5. 문제
- R과 성능 차이가 많이 난다.
 