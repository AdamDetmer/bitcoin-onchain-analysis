import json

nb_path = 'c:/Users/dell/Desktop/Studia/Sem_8/bitcoin-onchain-analysis/notebooks/arima_bitcoin.ipynb'
try:
    with open(nb_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    for i, cell in enumerate(data['cells']):
        print(f"\n========== Cell {i} : {cell['cell_type']} ==========")
        source = "".join(cell.get('source', []))
        # print up to 500 characters of source to keep it readable
        print(source[:500] + ('...' if len(source) > 500 else ''))
except Exception as e:
    print(e)
