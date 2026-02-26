import sys
import pandas as pd
from rapidfuzz import process, fuzz


def load_interests(csv_path: str) -> tuple[dict[str, list[str]], list[str], dict[str, list[str]]]:
    """
    Returns:
      - interest_map: interest -> [keywords...]
      - all_keywords: flattened list of keywords
      - keyword_to_interests: keyword -> [interests...]
    CSV columns required: interest, keyword
    """
    df = pd.read_csv(csv_path)

    if "interest" not in df.columns or "keyword" not in df.columns:
        raise ValueError("CSV must contain 'interest' and 'keyword' columns")

    interest_map: dict[str, list[str]] = {}
    keyword_to_interests: dict[str, list[str]] = {}

    for _, row in df.iterrows():
        interest = str(row["interest"]).strip()
        keyword = str(row["keyword"]).strip().lower()
        if not interest or not keyword:
            continue

        interest_map.setdefault(interest, []).append(keyword)
        keyword_to_interests.setdefault(keyword, []).append(interest)

    all_keywords = sorted(keyword_to_interests.keys())
    return interest_map, all_keywords, keyword_to_interests


def match_one_company(
    company: str,
    interest_map: dict[str, list[str]],
    all_keywords: list[str],
    keyword_to_interests: dict[str, list[str]],
    fuzzy_threshold: int,
) -> tuple[str, str, int]:
    """
    Returns: (matched_interests, best_keyword, best_score)
    matched_interests is '; '-separated interests.
    """
    c = (company or "").strip().lower()
    if not c:
        return ("", "", -1)

    # 1) Exact/contains match per interest
    matched = []
    for interest, keywords in interest_map.items():
        for kw in keywords:
            if kw and kw in c:
                matched.append(interest)
                break

    if matched:
        matched_unique = "; ".join(dict.fromkeys(matched))  # preserve order, unique
        return (matched_unique, "", 100)

    # 2) Fuzzy match against keywords (best single keyword)
    best = process.extractOne(c, all_keywords, scorer=fuzz.WRatio)
    if best is None:
        return ("", "", -1)

    best_keyword, best_score, _ = best
    if best_score < fuzzy_threshold:
        return ("", best_keyword, int(best_score))

    interests = keyword_to_interests.get(best_keyword, [])
    return ("; ".join(dict.fromkeys(interests)), best_keyword, int(best_score))


def run(input_xlsx: str, interests_csv: str, output_xlsx: str, fuzzy_threshold: int = 90) -> None:
    companies_df = pd.read_excel(input_xlsx)

    if "Company" not in companies_df.columns:
        raise ValueError("Input Excel must contain a 'Company' column")

    interest_map, all_keywords, keyword_to_interests = load_interests(interests_csv)

    out_matched = []
    out_best_kw = []
    out_best_score = []

    for val in companies_df["Company"]:
        company = "" if pd.isna(val) else str(val)
        matched, best_kw, best_score = match_one_company(
            company=company,
            interest_map=interest_map,
            all_keywords=all_keywords,
            keyword_to_interests=keyword_to_interests,
            fuzzy_threshold=fuzzy_threshold,
        )
        out_matched.append(matched)
        out_best_kw.append(best_kw)
        out_best_score.append("" if best_score < 0 else best_score)

    companies_df["Matched Interests"] = out_matched
    companies_df["Best Keyword"] = out_best_kw
    companies_df["Best Score"] = out_best_score

    companies_df.to_excel(output_xlsx, index=False)
    print(f"Wrote: {output_xlsx}")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python fuzzy_company_interest_matcher.py <input.xlsx> <interests.csv> <output.xlsx> [threshold]")
        sys.exit(1)

    input_file = sys.argv[1]
    interests_file = sys.argv[2]
    output_file = sys.argv[3]
    threshold = int(sys.argv[4]) if len(sys.argv) >= 5 else 90

    run(input_file, interests_file, output_file, fuzzy_threshold=threshold)