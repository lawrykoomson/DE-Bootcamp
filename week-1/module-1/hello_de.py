# ============================================
# MODULE 1 — My First Data Engineering Script
# GetSkills Network DE Bootcamp — Week 1
# ============================================

import sys
import os
from datetime import datetime

print("=" * 50)
print("   DATA ENGINEERING BOOTCAMP — WEEK 1")
print("=" * 50)
print(f"\n👋 Hello Data Engineer!")
print(f"\n🐍 Python Version:  {sys.version}")
print(f"📁 Working Dir:     {os.getcwd()}")
print(f"🕐 Script Run At:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

pipeline_name = "Ghana HR Ingestion Pipeline"
pipeline_version = "1.0.0"
author = "Lawrence Koomson"
status = "Active"

print(f"\n{'='*50}")
print("   PIPELINE INFORMATION")
print(f"{'='*50}")
print(f"📌 Pipeline:  {pipeline_name}")
print(f"🔢 Version:   {pipeline_version}")
print(f"👤 Author:    {author}")
print(f"✅ Status:    {status}")
print(f"\n🚀 Environment ready for Data Engineering!")