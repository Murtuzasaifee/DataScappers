import requests
import pdfplumber
from io import BytesIO
from datetime import datetime, timedelta
import json

def basic_stock_data(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolData&marketType=N&series=EQ&symbol={tickersymbol}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    json_data = response.json()

    equity = json_data["equityResponse"][0]

    last_price = equity["orderBook"]["lastPrice"]
    previous_close = equity["metaData"]["previousClose"]

    change = round(last_price - previous_close, 2)
    p_change = round(((last_price - previous_close) / previous_close) * 100, 2)

    output = {
        "companyName": equity["metaData"]["companyName"],
        "symbol": equity["metaData"]["symbol"],
        "lastPrice": last_price,
        "previousClose": previous_close,
        "dayHigh": equity["metaData"]["dayHigh"],
        "dayLow": equity["metaData"]["dayLow"],
        "yearHigh": equity["priceInfo"]["yearHigh"],
        "yearLow": equity["priceInfo"]["yearLow"],
        "VWAP": equity["metaData"]["averagePrice"],
        "change": change,
        "pChange": p_change,
        "totalTradedVolume": f"{equity['tradeInfo']['totalTradedVolume']:,}",
        "deliveryQuantity": f"{int(equity['tradeInfo']['deliveryquantity']):,}",
        "deliveryToTraded": equity["tradeInfo"]["deliveryToTradedQuantity"],
        "dailyVolatility": equity["priceInfo"]["cmDailyVolatility"],
        "adjustedPE": equity["secInfo"]["pdSectorPe"],
        "lastUpdatedDate": equity["lastUpdateTime"]
    }

    return output


def get_percentagechange_data(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getYearwiseData&symbol={tickersymbol}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    first = data[0] if isinstance(data, list) and len(data) > 0 else {}

    output = {
        "oneWeekChange": first.get("one_week_chng_per"),
        "oneMonthChange": first.get("one_month_chng_per"),
        "oneYearChange": first.get("one_year_chng_per"),
        "oneWeekDate": first.get("one_week_date"),
        "indexName": first.get("index_name"),
        "indexOneWeekChange": first.get("index_one_week_chng_per"),
        "indexOneMonthChange": first.get("index_one_month_chng_per"),
        "indexOneYearChange": first.get("index_one_year_chng_per")
    }

    return output

def get_financial_status(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getFinancialStatus&symbol={tickersymbol}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    results = []
    for item in data[:5]:
        record = {
            "quarter": item.get("to_date_MonYr"),
            "totalIncome": item.get("totalIncome"),
            "profitBeforeTax": item.get("reProLossBefTax"),
            "profitAfterTax": item.get("netProLossAftTax"),
            "earningsPerShare": item.get("eps")
        }
        results.append(record)
    return results

def get_voting_results(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getVotingResult&symbol={tickersymbol}&series_type=equity"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    # Get top 5
    top5 = data[:5]
    results = []
    for item in top5:
        record = {
            "voteDate": item.get("vrTimestamp"),
            "voteResolution": item.get("vrResolution"),
            "voteTotShares": item.get("vrTotSharesOnRec"),
            "voteInFavour": item.get("vrTotPercFor"),
            "voteAgainst": item.get("vrTotPercAgainst")
        }
        results.append(record)

    return results

def get_complaint_raised(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getInvestorComplaints&symbol={tickersymbol}&marketApiType=equities"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    # Get top 5
    top5 = data[:5]
    results = []
    for item in top5:
        record = {
            "compDate": item.get("date"),
            "compReceived": item.get("complRecv"),
            "compUnresolved": item.get("complUnres")
        }
        results.append(record)
    return results

def get_shareholding_details(tickersymbol):
    url = f"https://www.nseindia.com/api/corporate-share-holdings-master?index=equities&symbol={tickersymbol}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    # Get top 5
    top10 = data[:10]
    results = []
    for item in top10:
        record = {
            "holdingDate": item.get("date"),
            "promoterHolding": item.get("pr_and_prgrp"),
            "publicHolding": item.get("public_val"),
            "employeeHolding": item.get("employeeTrusts")
        }
        results.append(record)
    return results

def extract_pdf_text(pdf_url, max_pages=3):
    try:
        pdf_response = requests.get(pdf_url, headers={"User-Agent": "Mozilla/5.0"})
        pdf_bytes = BytesIO(pdf_response.content)

        extracted_text = ""
        with pdfplumber.open(pdf_bytes) as pdf:
            pages_to_read = min(len(pdf.pages), max_pages)
            for i in range(pages_to_read):
                page = pdf.pages[i]
                text = page.extract_text()
                if text:
                    extracted_text += text + "\n"
        return extracted_text.strip()
    except Exception as e:
        return f"PDF extraction failed: {str(e)}"


def get_corporate_announcements(tickersymbol):
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getCorporateAnnouncement&symbol={tickersymbol}&marketApiType=equities&noOfRecords=10"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    results = []
    for item in data:
        pdf_url = item.get("attchmntFile")

        details_text = ""
        if pdf_url:
            details_text = extract_pdf_text(pdf_url)

        record = {
            "date": item.get("exchdisstime"),
            "description": item.get("desc"),
            "details": details_text
        }
        results.append(record)
    return results

def get_last_trading_days(n=7):
    days = []
    today = datetime.today()
    while len(days) < n:
        if today.weekday() < 5:  # Monday-Friday
            days.append(today)
        today -= timedelta(days=1)
    days.reverse()
    return days


def get_volume_analysis(tickersymbol):
    trading_days = get_last_trading_days(7)
    from_date = trading_days[0].strftime("%d-%m-%Y")
    to_date = trading_days[-1].strftime("%d-%m-%Y")

    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getHistoricalTradeData&symbol={tickersymbol}&series=EQ&fromDate={from_date}&toDate={to_date}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    volume_data = []
    for item in data:
        volume_data.append({
            "date": item.get("mtimestamp"),
            "volume": int(item.get("chTotTradedQty", 0))
        })

    # Sort by date
    volume_data = sorted(volume_data, key=lambda x: datetime.strptime(x["date"], "%d-%b-%Y"))

    if len(volume_data) < 5:
        return {"error": "Not enough trading data"}

    today_volume = volume_data[-1]["volume"]

    prev_volumes = [x["volume"] for x in volume_data[:-1]]
    avg_prev_volume = sum(prev_volumes) / len(prev_volumes)

    avg_percentage = ((today_volume - avg_prev_volume) / avg_prev_volume) * 100

    result = {
        "volumes": volume_data,
        "average_volume": round(avg_percentage, 2)
    }
    return result


if __name__ == '__main__':
    # Example usage
    tickersymbol = "HDFCBANK"
    finalresult = {}

    data = basic_stock_data(tickersymbol)
    finalresult["basicProfile"] = data

    eqSymbol = "HDFCBANKEQN"
    data = get_percentagechange_data(eqSymbol)
    finalresult["priceChangePercentage"] = data

    data = get_financial_status(tickersymbol)
    finalresult["quarterlyResult"] = data

    data = get_voting_results(tickersymbol)
    finalresult["votingDetails"] = data

    data = get_complaint_raised(tickersymbol)
    finalresult["complaintDetails"] = data


    data = get_shareholding_details(tickersymbol)
    finalresult["shareholdingDetails"] = data

    data = get_volume_analysis(tickersymbol)
    finalresult["tradeVolume"] = data

    data = get_corporate_announcements(tickersymbol)
    finalresult["corporateAnnouncements"] = data

    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(finalresult, f, indent=2)

    print("Process completed successfully")