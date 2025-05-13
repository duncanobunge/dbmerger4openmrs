# OpenMRS Database Merger

This project contains an automated SQL-based merger script to consolidate OpenMRS databases from multiple distribute OpenMRS instances into a central OpenMRS database. The goal is to support unified data management and analytics across distributed sites.

## 📂 File Description

- `Openmrs_Merger_Script_2025_April_29.sql`:  
  The SQL script used to perform the monthly database merge from remote instances to the central OpenMRS database. It is optimized for production environments running Ubuntu with MySQL or MariaDB as the database engine.

## 🔄 Automation

A GitHub Actions workflow (`.github/workflows/monthly-merger.yml`) is included to schedule the merger process monthly. It assumes that a secure remote connection and credentials are configured via GitHub Secrets (see below).

## 🚀 Deployment Requirements

- Cloud VMs with Ubuntu OS
- OpenMRS instances running MySQL/MariaDB
- SSH access to source VMs
- GitHub Actions runners or external automation agent
- A central server with access to source databases

## 🔐 GitHub Secrets

Make sure to define the following GitHub secrets in your repository:

- `SSH_PRIVATE_KEY`: Private key for accessing remote instances
- `REMOTE_HOSTS`: Comma-separated IP addresses or hostnames of remote VMs
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `CENTRAL_DB_HOST`: Host of the central OpenMRS database

## 📅 Schedule

The merger runs on the **1st of every month at 2:00 AM UTC**.

---

## 👨‍💻 Maintainers

This repository is maintained by **Quadz Data Consultant**.  
*Your Home of Data.*

---

