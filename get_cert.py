from hashlib import sha1

def compute_hash(email):
    return sha1(email.encode('utf-8')).hexdigest()

def compute_certificate_id(email):
    email_clean = email.lower().strip()
    return compute_hash(email_clean + '_')

#a78ce9cc63c76c489814141469ea5943e83a39c5

cohort = 2024
course = 'dezoomcamp'
your_id = compute_certificate_id('rinika.krishnaswamy@gmail.com')
url = f"https://certificate.datatalks.club/{course}/{cohort}/{your_id}.pdf"
print(url)
