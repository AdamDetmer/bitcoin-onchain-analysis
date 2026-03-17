# Raport realizacji projektu – analiza danych blockchain Bitcoin

## Cel projektu
Celem projektu było pobranie, przetworzenie i przygotowanie danych on-chain oraz rynkowych Bitcoina w celu ich dalszego wykorzystania w modelach analitycznych (np. predykcji ceny).

---

## 1. Pobieranie danych (Data Ingestion)

Zaimplementowano moduł odpowiedzialny za pobieranie danych z zewnętrznych źródeł.

### Dane rynkowe
Źródło: Yahoo Finance  
Zakres: do 5 lat danych historycznych  

Pobierane dane:
- cena BTC (`price`)
- wolumen (`total_volume`)
- estymowana kapitalizacja (`market_cap`)

Dane zapisywane są do pliku:
data/raw/bitcoin_daily_market_data.csv

### Dane on-chain
Źródło: Blockchain.com API  

Pobierane metryki:
- liczba transakcji (`n-transactions`)
- hash rate (`hash-rate`)
- wolumen transakcji (`estimated-transaction-volume-usd`)

Dane zapisywane są do pliku:
data/raw/bitcoin_onchain_data.csv

---

## 2. Przetwarzanie danych (Preprocessing)

Zaimplementowano moduł integracji i czyszczenia danych.

### Łączenie danych
- połączenie danych rynkowych i on-chain po kolumnie `date`
- zastosowano INNER JOIN (zachowane tylko wspólne daty)

### Czyszczenie danych
- sortowanie po dacie
- interpolacja liniowa w przypadku brakujących wartości

### Wynik
Zapis do:
data/processed/bitcoin_master.csv

---

## 3. Feature Engineering

Dodano cechy wykorzystywane w analizie i modelowaniu.

### Wskaźniki techniczne
- Log Return (logarytmiczna stopa zwrotu)
- SMA 7 dni (`sma_7`)
- SMA 30 dni (`sma_30`)
- RSI (14 dni)

### Czyszczenie
- usunięcie wartości NaN powstałych podczas obliczeń

### Wynik
Zapis do:
data/processed/bitcoin_final.csv

---

## 4. Walidacja danych (Test Pipeline)

Dodano moduł sprawdzający poprawność danych.

Sprawdzane elementy:
- brak wartości NaN
- obecność kluczowych kolumn:
  - price
  - log_return
  - rsi
  - n-transactions
  - hash-rate
- ciągłość dat (brak luk w szeregu czasowym)

---

## 5. Pipeline end-to-end

Zaimplementowano pełny pipeline uruchamiany z jednego pliku.

Kolejność operacji:
1. Pobranie danych
2. Przetwarzanie i łączenie
3. Generowanie cech
4. Walidacja danych

Pipeline uruchamiany z pliku:
main.py

---

## 6. Pokrycie wymagań projektowych

| Wymaganie | Status | Uwagi |
|----------|--------|------|
| Przepływy BTC na/z giełd | Nie | brak w obecnym kodzie |
| Przepływy stablecoin | Nie | brak |
| Ruchy wielorybów | Nie | brak |
| HODL ratio | Nie | brak |
| Lightning Network | Nie | brak |
| Liczba aktywnych adresów | Częściowo | brak bezpośredniego źródła |
| Wolumen transakcji | Tak | dostępne w danych |
| Liczba transakcji | Tak | dostępne w danych |

---

## Podsumowanie

Zrealizowano:
- pełny pipeline danych (ingestion → preprocessing → feature engineering → walidacja)
- integrację danych rynkowych i on-chain
- przygotowanie danych do modeli analitycznych

Do dalszego rozwoju:
- dodanie danych zaawansowanych on-chain (np. whale activity, exchange flows)
- integracja dodatkowych źródeł danych (np. Glassnode)
- budowa modelu predykcyjnego