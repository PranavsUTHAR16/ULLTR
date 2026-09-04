import json
import os
import subprocess

def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    nifty_path = os.path.join(root, "nifty_option_symbols.json")
    equity_path = os.path.join(root, "equity_symbols.json")
    cfg_path = os.path.join(root, "collector", "config.json")

    spot_indices = ["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX", "NSE_INDEX|Nifty Bank", "NSE_INDEX|India VIX"]
    all_syms = set(spot_indices)

    if os.path.exists(nifty_path):
        with open(nifty_path) as f:
            opts = json.load(f)
        for k, v in opts.items():
            for s in v.get("symbols", []):
                all_syms.add(s)

    if os.path.exists(equity_path):
        with open(equity_path) as f:
            eqs = json.load(f)
        for k in eqs.keys():
            all_syms.add(k)

    with open(cfg_path) as f:
        cfg = json.load(f)

    cfg["instruments"] = sorted(list(all_syms))
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)

    inst_count = len(cfg["instruments"])
    print(f"✅ Updated collector/config.json with {inst_count} instruments (Indices, Options, Futures, NIFTY 50 & SENSEX 30 Equities)!")

if __name__ == "__main__":
    main()
