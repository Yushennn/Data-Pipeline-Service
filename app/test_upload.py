import httpx

def test_api():
    url = "http://127.0.0.1:8000/upload-dataset"
    
    # 1. Prepare dummy files (creating them in memory for testing)
    files = [
        ("files", ("test1.csv", b"id,value\n1,100", "text/csv")),
        ("files", ("test2.csv", b"id,value\n2,200", "text/csv"))
    ]
    
    # 2. Prepare Form data
    data = {
        "weight_class_M58": 1.0,
        "weight_class_M68": 2.0,
        "weight_class_M80": 3.0,
        "weight_class_M80p": 4.0
    }
    
    # 3. Send the POST request
    print("Sending request to API...")
    response = httpx.post(url, files=files, data=data)
    
    # 4. Print the result
    print(f"Status Code: {response.status_code}")
    print("Response JSON:")
    print(response.json())

if __name__ == "__main__":
    test_api()