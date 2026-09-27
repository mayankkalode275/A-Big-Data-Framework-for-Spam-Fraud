"""
Dataset Generator for Big Data Spam and Fraud Identification Project
Semester 7 Mumbai University Computer Engineering

Generates realistic communication dataset with natural feature overlap between
Normal, Spam, and Fraud classes (URLs, digits, keywords, length variations).
"""

import csv
import os
import random
from datetime import datetime, timedelta

def generate_communications_dataset(filename="data/sample_communications.csv", num_records=3000, seed=42):
    random.seed(seed)
    
    # 1. Normal Communications Templates (includes legitimate URLs, numbers, and transactional words)
    normal_templates = [
        "Hey, are we still meeting for the Big Data lecture at 2 PM in Lab 304?",
        "Please find attached the project submission guidelines for Semester 7.",
        "Can you send me the practical lab manual for Apache Spark?",
        "Don't forget to submit the assignment before midnight today.",
        "Hi Team, the project review meeting has been rescheduled to tomorrow morning at 10 AM.",
        "Thanks for sharing the study notes for the upcoming viva exams.",
        "Are you available for a quick phone call regarding the group project?",
        "The professor updated the syllabus on the college portal. Check https://portal.somaiya.edu/syllabus",
        "Let's grab lunch after the distributed systems lab session.",
        "Happy Birthday! Hope you have a fantastic day ahead with family.",
        "Your Amazon order #408-1928374 of $45.50 has been delivered. Track at https://www.amazon.in/order-tracking",
        "Your Uber ride is arriving in 3 minutes. Driver: Suresh (MH-02-AB-1234).",
        "Reminder: Library books are due for return by Friday 5 PM.",
        "Hi, please check your email or visit https://github.com/mumbai-univ/big-data-proj for project slides.",
        "Good morning! Hope you have a productive week ahead.",
        "Here is the Zoom link for today's Big Data workshop: https://zoom.us/j/9812345678 Passcode: 102938",
        "The exam timetable for 7th semester engineering has been published on portal.",
        "Please verify your attendance record on the college ERP portal before Friday.",
        "Hey bro, did you complete the Hadoop installation step on your laptop?",
        "Meeting confirmed for tomorrow at 11 AM in Lab 304.",
        "Your salary payment of $3,200 was credited to your bank account #8912. Statement at https://mybank.com/stmt",
        "I paid the exam registration fee of $120 via net banking. Transaction ID #TXN98123.",
        "My bank branch is closed tomorrow due to a public holiday.",
        "The store issued a full refund of $35 for your returned item.",
        "Our project advisor offered extra lab hours for Apache Spark model tuning."
    ]

    # 2. Spam Communications Templates (some text-only, some single keyword, some with URLs)
    spam_templates = [
        "CONGRATS! You have won a free iPhone 15 Pro Max! Click http://win-iphone.xyz to claim now!",
        "Special Offer: Get 80% OFF on all branded shoes! Visit www.superdeals.com before offer ends!",
        "Earn $500/day working from home with no investment required! Sign up at http://easycash.net",
        "Flat 50% discount on online coding courses! Limited seats remaining. Enrol today at www.learnfast.org",
        "Exclusive loan offer at 0% interest rate! Apply instantly at http://quickloan-approval.com",
        "Win a jackpot of $10,000 in our lucky draw! Text WIN to 58888 now to enter!",
        "Unsubscribe from daily promotional alerts by clicking http://optout-deals.com or replying STOP.",
        "Huge End-of-Season Sale! Buy 1 Get 2 Free on all clothing items at www.fashionstore.com",
        "Get instant credit score check for FREE! No hidden charges. Visit http://free-credit-check.org",
        "Congratulations! Your mobile number won 50,000 reward points! Redeem at www.rewardpoints.com",
        "Hot summer deal! Flight tickets starting at just $49! Book now at http://cheapflights.net",
        "Boost your Instagram followers by 10k in 24 hours! Visit www.socialgrow.xyz now!",
        "Limited time clearance sale! Up to 90% off electronics at http://mega-sale-online.com",
        "Try our new weight loss supplement for FREE! Order trial pack at www.fitlife.net",
        "Exclusive VIP pass for tech conference! Claim your free seat at http://techvip-pass.com",
        "BIG WEEKEND SALE! Get 50% discount on all branded apparel! Reply YES for coupon code.",
        "Special discount on summer online courses. Limited seats available today! Call 1800-9988-77.",
        "Free home delivery on all grocery orders above $30. Order now at local store!",
        "Claim your $100 shopping voucher today! Reply CLAIM to 56767 to activate.",
        "Super offer! Get unlimited streaming for just $5 per month. Sign up today."
    ]

    # 3. Fraud Communications Templates (some without URLs, some call-only, some with URLs)
    fraud_templates = [
        "CRITICAL ALERT: Your HDFC Bank account #9872 has been SUSPENDED due to pending KYC! Verify NOW at http://hdfc-kyc-update.net to avoid permanent block!",
        "SECURITY NOTICE: Your SBI net banking account was accessed from unauthorized location. Click http://sbi-login-verify.com to secure your account immediately!",
        "URGENT: Your bank account will be blocked within 24 hours due to missing PAN card details. Update immediately at http://bank-pan-verification.org",
        "ATTENTION: Your electricity bill of $240 is unpaid. Power supply will be disconnected at 9 PM. Pay immediately at http://power-bill-pay.com or call 9876543210!",
        "ALERT: Your Paytm Wallet has been restricted. Share your OTP 482910 with our support agent to unblock your wallet instantly.",
        "TAX REFUND NOTICE: You are eligible for an income tax refund of $1,450. Click http://income-tax-refund-portal.net to claim your refund to bank account.",
        "FRAUD ALERT: Suspicious transaction of $899 detected on your Credit Card ending 4321. If not done by you, verify details at http://card-security-alert.net",
        "INCOME TAX WARNING: Legal action initiated against your PAN for tax evasion. Call immediately at +91-9988776655 to settle compliance.",
        "URGENT SECURITY: Your WhatsApp account code is 918-243. Never share this code with anyone. Click http://whatsapp-verify-security.com to protect.",
        "DEAR CUSTOMER: Your SIM card will be deactivated today due to non-verification. Complete e-KYC now at http://sim-kyc-update-online.com",
        "PRIZE WINNER ALERT: You won 25 Lakh INR in KBC Lucky Draw! Deposit processing fee of $200 at http://kbc-prize-claim.xyz to release funds!",
        "BANK ALERT: Your debit card has been disabled due to wrong PIN entries. Reactivate your card immediately at http://debitcard-activate-now.com",
        "URGENT: Your Amazon account has been locked due to suspicious purchase of $1,200. Verify identity at http://amazon-account-security-check.com",
        "FINAL NOTICE: Court summons issued for unpaid loan. Contact advocate immediately or visit http://legal-notice-portal.com to avoid arrest.",
        "SBI URGENT: Download SBI Safety App APK immediately from http://sbi-secure-app.xyz to prevent account hacking!",
        "SECURITY ALERT: Your net banking account #7819 was accessed from suspicious IP. Call customer care +919876543210 immediately to unblock.",
        "URGENT: Your debit card PIN was entered incorrectly 3 times. Visit your local bank branch or call 1800-112-211 immediately.",
        "Your Paytm wallet is restricted due to incomplete e-KYC. Reply with OTP code 882910 to unblock immediately.",
        "BANK NOTICE: Your account #4092 is suspended. Please verify identity at nearest branch or call +919988776655.",
        "URGENT: Income tax department flagged your account for unpaid dues. Contact officer at +919820112233."
    ]

    senders = [
        "+919820112233", "+919876543210", "+918877665544", "+917766554433", "+919988776655",
        "support@bank-verify.net", "info@superdeals.com", "alerts@security-update.org",
        "admin@college.edu.in", "student1@somaiya.edu", "suresh.driver@uber.com",
        "offers@megasale.xyz", "kyc-alert@fast-verify.com", "service@amazon.in",
        "refund@tax-gov.org", "friend@gmail.com", "colleague@vti.edu.in"
    ]

    receivers = [
        f"user_{i}@example.com" if i % 2 == 0 else f"+91912345{i:04d}" for i in range(100, 500)
    ]

    comm_types = ["SMS", "Email", "WhatsApp", "VoIP"]
    start_date = datetime(2026, 9, 1, 8, 0, 0)
    
    rows = []
    
    for i in range(1, num_records + 1):
        msg_id = f"MSG{10000 + i}"
        
        # Distribution: 60% Normal, 25% Spam, 15% Fraud
        rand_val = random.random()
        if rand_val < 0.60:
            label = "Normal"
            msg = random.choice(normal_templates)
            # 12% chance of adding casual numerical note or link variation to Normal
            if random.random() < 0.12:
                msg += f" Note ref: #{random.randint(100, 999)}"
        elif rand_val < 0.85:
            label = "Spam"
            msg = random.choice(spam_templates)
            # 15% chance of adding reference number or mild wording
            if random.random() < 0.15:
                msg += f" Code: {random.randint(1000, 9999)}"
        else:
            label = "Fraud"
            msg = random.choice(fraud_templates)
            # 15% chance of adding urgent reference code
            if random.random() < 0.15:
                msg += f" Case: #{random.randint(10000, 99999)}"
            
        sender = random.choice(senders)
        receiver = random.choice(receivers)
        comm_type = random.choice(comm_types)
        
        random_minutes = random.randint(0, 25 * 24 * 60)
        timestamp = (start_date + timedelta(minutes=random_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        
        rows.append({
            "message_id": msg_id,
            "sender": sender,
            "receiver": receiver,
            "message": msg,
            "timestamp": timestamp,
            "communication_type": comm_type,
            "label": label
        })
        
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["message_id", "sender", "receiver", "message", "timestamp", "communication_type", "label"])
        writer.writeheader()
        writer.writerows(rows)
        
    print(f"[SUCCESS] Dataset successfully generated with {len(rows)} realistic records at '{filename}'.")

if __name__ == "__main__":
    generate_communications_dataset()
