import json

nb_path = 'c:/Users/dell/Desktop/Studia/Sem_8/bitcoin-onchain-analysis/notebooks/arima_bitcoin.ipynb'
out_path = 'c:/Users/dell/Desktop/Studia/Sem_8/bitcoin-onchain-analysis/nb_output.txt'

try:
    with open(nb_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    with open(out_path, 'w', encoding='utf-8') as f_out:
        for i, cell in enumerate(data['cells']):
            f_out.write(f"\n========== Cell {i} : {cell['cell_type']} ==========\n")
            source = "".join(cell.get('source', []))
            # Write up to 500 chars to avoid massive outputs, but enough to see what it is about
            f_out.write(source[:500])
            if len(source) > 500:
                f_out.write("\n... [truncated] ...\n")
            f_out.write("\n")
    print("Done")
except Exception as e:
    print(e)
