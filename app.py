import os
import json
import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs
from flask import Flask, jsonify, request, render_template, send_file
import tempfile
import xlsxwriter
from io import BytesIO
import time
from threading import Thread
import sys
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import math
import numpy as np
import re
from flask import render_template


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


# Flag to track if a job is currently running
job_running = False
last_run_time = None
latest_report_bytes = None

SECRET_PNON = "SHOWALL"   

def get_to_date():
    """Get the to_date from metadata sheet B2, or current UTC time if blank/invalid"""
    try:
        sh = gc.open(SPREADSHEET_NAME)
        metadata_sheet = sh.worksheet("Metadata")
        to_date_cell = metadata_sheet.cell(2, 2).value
        
        if to_date_cell:
            # Validate format by trying to parse it
            datetime.strptime(to_date_cell, "%Y-%m-%dT%H:%M:%S")
            return to_date_cell
        
        return datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, Exception):
        # Invalid format or other error - use current time
        return datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%dT%H:%M:%S")



def fetch_releases(from_date=None, to_date=None, PPON=None):
    all_releases = []
    page_count = 0
    base_url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
    
    

    logger.info(f"Fetching releases from {from_date} to {to_date}")

    params = {
        'updatedFrom': from_date,
        'updatedTo': to_date,
        'limit': 100
    }
    error_occurred = False # Track fetch errors
    
    while True:
        page_count += 1
        logger.info(f"Fetching page {page_count} (total records so far: {len(all_releases)})")
        
        try:
            # Add timeout to prevent hanging
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()  # Raises an error for bad status codes

            # Pre-process the response to fix invalid number formatting
            fixed_json = re.sub(r'"(amount|amountGross|value)": 0+([1-9]\d*)', r'"\1": \2', response.text)
            # Handle case of all zeros
            fixed_json = re.sub(r'"(amount|amountGross|value)": 0+\b', r'"\1": 0', fixed_json)
            
            try:
                data = json.loads(fixed_json)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error on page {page_count}")
                logger.error(f"Error details: {str(e)}")
                logger.error(f"Response text snippet: {response.text[:1000]}...")  # First 1000 chars
                logger.error(f"Response content type: {response.headers.get('content-type', 'unknown')}")
                start = max(0, e.pos - 100)
                end = min(len(response.text), e.pos + 100)
                logger.error(response.text[start:end])
                break

            releases = data.get('releases', [])
            if not releases:
                logger.info("No more releases found")
                break
                
            # Filter for your organization
            if PPON != SECRET_PNON:
                org_releases = [
                r for r in releases 
                if (r.get("buyer", {}).get("id") == PPON or 
                    (r.get("buyer", {}).get("id") is None and 
                    any(p.get("id") == PPON for p in r.get("parties", []))))
                ]
                logger.info(f"Page {page_count}: Found {len(org_releases)} releases for your organization out of {len(releases)} total")
                all_releases.extend(org_releases)
            else:
                org_releases = releases
            
        
            # Check for next page
            next_url = data.get('links', {}).get('next')
            if not next_url:
                logger.info("No more pages available")
                break
            
            # Extract cursor from next_url for pagination
            parsed = urlparse(next_url)
            cursor = parse_qs(parsed.query).get('cursor', [None])[0]
            if not cursor:
                logger.info("No cursor found in next URL")
                break
            
            params['cursor'] = cursor

            # Add a small delay between requests to be nice to the API
            time.sleep(1)

        except requests.Timeout:
            logger.error(f"Request timed out on page {page_count}")
            error_occurred = True
            break
        except requests.RequestException as e:
            logger.error(f"Request failed on page {page_count}: {str(e)}")
            error_occurred = True 
            break
    
    logger.info(f"Completed fetch: Found {len(all_releases)} total releases for your organization")
    return all_releases, error_occurred


def update_closed_unawarded_notices():
    try:
        logger.info("Updating closed unawarded notices")

        if tender_df.empty:
            logger.info("No tender notices found")
            return True, "No tender notices to analyze"

        # Get latest UK4 notice for each OCID (handling updates)
        uk4_notices = tender_df[tender_df['Notice Type'] == 'UK4'].copy()
        

        if uk4_notices.empty:
            logger.info("No UK4 notices found")
            return True, "No UK4 notices to analyze"
        
        latest_uk4 = uk4_notices.sort_values('Published Date').groupby('OCID').last()

        # Filter for closed tenders (submission deadline < current date)
        current_date = datetime.now(timezone.utc)
        closed_tenders = latest_uk4[
            pd.to_datetime(latest_uk4['Submission Deadline'], format='%Y-%m-%dT%H:%M:%S%z', utc=True) < current_date
        ]
        if not procurement_terminations_df.empty and 'OCID' in procurement_terminations_df.columns:
            terminated_ocids = set(procurement_terminations_df['OCID'].dropna())
            # Exclude any closed tenders whose OCID is in terminated_ocids
            closed_tenders = closed_tenders[~closed_tenders.index.isin(terminated_ocids)]
            logger.info(f"Excluded {len(closed_tenders[closed_tenders.index.isin(terminated_ocids)])} closed tenders from terminated procurements")

        if closed_tenders.empty:
            logger.info("No closed tenders found")
            return True, "No closed tenders to analyze"

        # If award_df is empty, all closed tenders are unawarded
        if award_df.empty:
            unawarded = closed_tenders
            logger.info("No award notices found - treating all closed tenders as unawarded")
        else:
            # Get OCIDs with award notices
            awarded_ocids = set(award_df[award_df['Notice Type'].isin(['UK6', 'UK7'])]['OCID'])
            # Filter for closed tenders without award notices
            unawarded = closed_tenders[~closed_tenders.index.isin(awarded_ocids)]

        # Prepare data for closed notices sheet
        closed_unawarded = unawarded.reset_index()[
            ['OCID', 'Notice Type', 'Notice Title', 'Submission Deadline', 'Published Date', 
             'Value ex VAT', 'Contracting Authority', 'Contact Name', 'Contact Email']
        ]
        closed_unawarded['Date Added to Report'] = current_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        closed_unawarded['Days Since Closed'] = (
            current_date - pd.to_datetime(closed_unawarded['Submission Deadline'], format='%Y-%m-%dT%H:%M:%S%z', utc=True)
        ).dt.days
        closed_unawarded['Status'] = closed_unawarded['Days Since Closed'].apply(
            lambda x: "Recently Closed" if x <= 30 else "Overdue Award Notice"
        )

        # Update sheet
        if not closed_unawarded.empty:
            logger.info(f"Found {len(closed_unawarded)} closed tenders without award notices")
            # Clear existing data
            closed_sheet.clear()
            # Update with new data
            values = [closed_unawarded.columns.values.tolist()] + closed_unawarded.values.tolist()
            closed_sheet.update(values=values, range_name='A1')
        else:
            logger.info("No closed tenders without award notices found")

        return True, "Closed unawarded notices updated successfully"
    
    except Exception as e:
        logger.error(f"Error updating closed unawarded notices: {str(e)}")
        return False, str(e)


def fetch_and_process_data(from_date, to_date, PPON):
    global job_running, last_run_time
    global latest_report_bytes
    latest_report_bytes = None
    
    # Set flag to indicate job is running
    job_running = True
    try:

        logger.info("Starting data fetch and processing job")

        # Get releases from API
        releases, fetch_error = fetch_releases(from_date=from_date, to_date=to_date, PPON=PPON)
        logger.info(f"Found {len(releases)} releases to process")

        if fetch_error:
            logger.error("Fetch did not complete successfully. Sheets will NOT be updated and fetch date will NOT be advanced.")
            update_last_fetch_status("Fetch failed")
            return False, "Fetch failed partway; no updates made."

        # Initialize results lists
        planning_results = []  # UK1-3
        tender_results = []    # UK4
        award_notice_results = []    # UK5-7
        lot_results = []
        award_results = []
        procurement_termination_results = [] # UK12


        for release in releases:
            contract_docs = release.get("contracts", [])[0].get("documents", []) if release.get("contracts") else []
            award_docs = release.get("awards", [])[0].get("documents", []) if release.get("awards") else []
            tender_docs = release.get("tender", {}).get("documents", [])
            planning_docs = release.get("planning", {}).get("documents", [])
            

            # Get documents in priority order
            if contract_docs:
                documents = contract_docs
            elif award_docs:
                documents = award_docs
            elif tender_docs:
                documents = tender_docs
            elif planning_docs:
                documents = planning_docs
            else:
                continue

            notice_type = documents[-1].get("noticeType")

            awards = release.get("awards", [])
            if awards:
                # If there is an awards section then:
                # - For UK4, you wouldn't expect any awards.
                # - For UK12, there should be awards.
                # If any award has a status of 'cancelled', treat it as UK12:
                if release.get("awards", [{}])[0].get("status") == "cancelled":
                    documents = release.get("tender", {}).get("documents", [])
                    notice_type = documents[-1].get("noticeType")

            lots = release.get("tender", {}).get("lots", [])
            is_update = any('update' in tag.lower() for tag in release.get('tag', []))

            if notice_type in ["UK1", "UK2", "UK3"]:
                if "planning" in release:
                    # Extract notice fields
                    notice_fields = {
                    "OCID": release.get("ocid", "N/A"),
                    "Notice Type": notice_type,
                    "Is Update": is_update,
                    "Published Date": release.get("date", "N/A"),
                    "Notice ID": release.get("id", "N/A"),
                    "Reference": release.get("tender", {}).get("id", "N/A"),
                    "Notice Title": release.get("tender", {}).get("title", "N/A"),
                    "Notice Description": release.get("tender", {}).get("description", "N/A"),
                    "Value ex VAT": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                    "Value inc VAT": release.get("tender", {}).get("value", {}).get("amountGross", "N/A"),
                    "Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                    "Threshold": "Above the relevant threshold" if release.get("tender", {}).get("aboveThreshold", False) else "Below the relevant threshold",
                    # Assume contract dates are same for all lots
                    "Contract Start Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A"),
                    "Contract End Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A"),
                    "Publication date of tender notice (estimated)": release.get("tender", {}).get("communication", {}).get("futureNoticeDate", "N/A"),
                    "Main Category": release.get("tender", {}).get("mainProcurementCategory", "N/A"),
                    "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                        else "See lots sheet for CPV codes",
                    "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                    "Enquiry Deadline": release.get("planning", {}).get("milestones", [{}])[0].get("dueDate", "N/A"),
                    "Estimated Award Date": release.get("tender", {}).get("awardPeriod", {}).get("endDate", "N/A"),
                    "Award Criteria": (
                            "Detailed in lots sheet" if len(lots) > 1
                            else (
                                release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("description", "N/A")
                                if not release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("criteria")
                                else "Refer to notice for detailed weightings"
                            )
                        ),
                    "Framework Agreement": (
                            "Closed Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "closed"
                            else "Open Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "open"
                            else "N/A"
                        ), 
                    "Call off method": (
                            "With competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withReopeningCompetition"
                            else "Without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withoutReopeningCompetition"
                            else "Either with or without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withAndWithoutReopeningCompetition"
                            else "N/A"
                        ),
                    "Procedure Type": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                    "Procedure Description": release.get("tender", {}).get("procedure", {}).get("features", "N/A"),
                    "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                    "PPON": release.get("buyer", {}).get("id", "N/A"),
                    "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                    "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),

                    }
                    planning_results.append(notice_fields)

                    if len(lots) > 1:  # Only create lot entries for multiple lots
                        for idx, lot in enumerate(lots, 1):
                            lot_fields = { 
                                "OCID": release.get("ocid", "N/A"),
                                "Notice Type": notice_type,
                                "Is Update": is_update,
                                "Lot Number": idx,
                                "Lot Title": lot.get("title", "N/A"),
                                "Lot Description": lot.get("description", "N/A"),
                                "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                                "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                                "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                                "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                                "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                                "SME Suitable": lot.get("suitability", {}).get("sme", False),
                                "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                                "Award Criteria": (
                                    lot.get("awardCriteria", {}).get("description", "N/A")
                                    if not lot.get("awardCriteria", {}).get("criteria")
                                    else "Refer to notice for detailed weightings"
                                    ),
                                "CPV Code": (
                                    next(
                                        (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                        for item in release.get("tender", {}).get("items", [])
                                        if item.get("relatedLot") == lot.get("id")),
                                        "N/A"
                                    )
                                ),
                            }
                            lot_results.append(lot_fields)


            elif notice_type in ["UK4"]:
                logger.debug(f"UK4 dates for {release.get('ocid')}: " +
                f"Start={release.get('tender', {}).get('lots', [{}])[0].get('contractPeriod', {}).get('startDate', 'N/A')}, " +
                f"End={release.get('tender', {}).get('lots', [{}])[0].get('contractPeriod', {}).get('endDate', 'N/A')}")
                
                # Extract notice fields
                notice_fields = {
                    "OCID": release.get("ocid", "N/A"),
                    "Notice Type": notice_type,
                    "Is Update": is_update,
                    "Published Date": release.get("date", "N/A"),
                    "Notice ID": release.get("id", "N/A"),
                    "Reference": release.get("tender", {}).get("id", "N/A"),
                    "Notice Title": release.get("tender", {}).get("title", "N/A"),
                    "Notice Description": release.get("tender", {}).get("description", "N/A"),
                    "Value ex VAT": release.get("tender", {}).get("value", {}).get("amount", "N/A"),
                    "Value inc VAT": release.get("tender", {}).get("value", {}).get("amountGross", "N/A"),
                    "Currency": release.get("tender", {}).get("value", {}).get("currency", "N/A"),
                    "Threshold": "Above the relevant threshold" if release.get("tender", {}).get("aboveThreshold", False) else "Below the relevant threshold",
                    "Contract Start Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A"),
                    "Contract End Date": release.get("tender", {}).get("lots", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A"),
                    "Renewal": release.get("tender", {}).get("renewal", {}).get("description", "N/A"),
                    "Options": release.get("tender", {}).get("options", {}).get("description", "N/A"),
                    "Main Category": release.get("tender", {}).get("mainProcurementCategory", "N/A"),
                    "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                    else "See lots sheet for CPV codes",
                    "Particular Suitability": (
                        ", ".join(filter(None, [
                            "SME" if release.get("tender", {}).get("lots", [{}])[0].get("suitability", {}).get("sme") else None,
                            "VCSE" if release.get("tender", {}).get("lots", [{}])[0].get("suitability", {}).get("vcse") else None
                        ])) or "N/A"
                    ),
                    "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                    "Submission Method": release.get("tender", {}).get("submissionMethodDetails", "N/A"),
                    "Enquiry Deadline": release.get("tender", {}).get("enquiryPeriod", {}).get("endDate", "N/A"),
                    "Estimated Award Date": release.get("tender", {}).get("awardPeriod", {}).get("endDate", "N/A"),
                    "Award Criteria": (
                        "Detailed in lots sheet" if len(lots) > 1
                        else (
                            release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("description", "N/A")
                            if not release.get("tender", {}).get("lots", [{}])[0].get("awardCriteria", {}).get("criteria")
                            else "Refer to notice for detailed weightings"
                        )
                    ),
                    "Framework Agreement": (
                        "Closed Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "closed"
                        else "Open Framework" if release.get("tender", {}).get("techniques", {}).get("type") == "open"
                        else "N/A"
                    ), 
                    "Call off method": (
                        "With competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withReopeningCompetition"
                        else "Without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withoutReopeningCompetition"
                        else "Either with or without competition" if release.get("tender", {}).get("techniques", {}).get("frameworkAgreement", {}).get("method") == "withAndWithoutReopeningCompetition"
                        else "N/A"
                    ),
                    "Procedure Type": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                    "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                    "PPON": release.get("buyer", {}).get("id", "N/A"),
                    "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                    "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),
                }
                
                tender_results.append(notice_fields)
                
                
                if len(lots) > 1:  # Only create lot entries for multiple lots
                    for idx, lot in enumerate(lots, 1):
                        lot_fields = { 
                            "OCID": release.get("ocid", "N/A"),
                            "Notice Type": notice_type,
                            "Is Update": is_update,
                            "Lot Number": idx,
                            "Lot Title": lot.get("title", "N/A"),
                            "Lot Description": lot.get("description", "N/A"),
                            "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                            "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                            "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                            "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                            "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                            "SME Suitable": lot.get("suitability", {}).get("sme", False),
                            "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                            "Award Criteria": (
                                lot.get("awardCriteria", {}).get("description", "N/A")
                                if not lot.get("awardCriteria", {}).get("criteria")
                                else "Refer to notice for detailed weightings"
                                ),
                            "CPV Code": (
                                    next(
                                    (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                    for item in release.get("tender", {}).get("items", [])
                                    if item.get("relatedLot") == lot.get("id")),
                                    "N/A"
                                )
                            ),
                        }
                        lot_results.append(lot_fields)
        
            elif notice_type in ["UK12"]:
                notice_fields = {
                    "OCID": release.get("ocid", "N/A"),
                    "Notice Type": notice_type,
                    "Is Update": is_update,
                    "Published Date": release.get("date", "N/A"),
                    "Notice ID": release.get("id", "N/A"),
                    "Reference": release.get("tender", {}).get("id", "N/A"),
                    "Notice Title": release.get("tender", {}).get("title", "N/A"),
                    "Cancellation Reason": release.get("awards", [{}])[0].get("statusDetails", "N/A")
                }
                procurement_termination_results.append(notice_fields)
            
            
            elif notice_type in ["UK5", "UK6", "UK7"]:
                # Extract notice fields
                notice_fields = {
                    "OCID": release.get("ocid", "N/A"),
                    "Notice Type": notice_type,
                    "Is Update": is_update,
                    "Published Date": release.get("date", "N/A"),
                    "Notice ID": release.get("id", "N/A"),
                    "Reference": release.get("tender", {}).get("id", "N/A"),
                    "Notice Title": release.get("tender", {}).get("title", "N/A"),
                    "Notice Description": release.get("tender", {}).get("description", "N/A"),
                    "Awarded Amount ex VAT": (
                        release.get("contracts", [{}])[0].get("value", {}).get("amount", "N/A") 
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("amount", "N/A")
                    ),
                    "Awarded Amount inc VAT": (
                        release.get("contracts", [{}])[0].get("value", {}).get("amountGross", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("amountGross", "N/A")
                    ),
                    "Currency": (
                        release.get("contracts", [{}])[0].get("value", {}).get("currency", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("value", {}).get("currency", "N/A")
                    ),
                    "Threshold": (
                        "Above the relevant threshold" 
                        if (notice_type == "UK7" and release.get("contracts", [{}])[0].get("aboveThreshold", False))
                        or (notice_type in ["UK5", "UK6"] and release.get("awards", [{}])[0].get("aboveThreshold", False))
                        else "Below the relevant threshold"
                    ),
                    "Earliest date the contract will be signed": (
                        release.get("awards", [{}])[0].get("milestones", [{}])[0].get("dueDate", "N/A") 
                        if release.get("awards", [{}])[0].get("milestones", [{}])[0].get("type") == "futureSignatureDate" 
                        else "N/A"
                    ),
                    "Contract Start Date": (
                        release.get("contracts", [{}])[0].get("period", {}).get("startDate", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("contractPeriod", {}).get("startDate", "N/A")
                    ),
                    "Contract End Date": (
                        release.get("contracts", [{}])[0].get("period", {}).get("endDate", "N/A")
                        if notice_type == "UK7"
                        else release.get("awards", [{}])[0].get("contractPeriod", {}).get("endDate", "N/A")
                    ),
                    "Contract Signature Date": (
                        release.get("contracts", [{}])[0].get("dateSigned", "N/A")
                    ),
                    "Suppliers": (
                        ", ".join([supplier.get("name", "N/A") for supplier in release.get("awards", [{}])[0].get("suppliers", [])])
                    ),
                    "Supplier ID": (
                        ", ".join([supplier.get("id", "N/A") for supplier in release.get("awards", [{}])[0].get("suppliers", [])])
            ),
                    "Main Category": (
                        "See awards sheet" 
                        if notice_type in ["UK6", "UK7"]
                        else release.get("awards", [{}])[0].get("mainProcurementCategory", "N/A")
                    ),
                    "CPV Code": release.get("tender", {}).get("items", [{}])[0].get("additionalClassifications", [{}])[0].get("id", "N/A") if len(lots) == 1
                        else "See lots sheet for CPV codes",
                    "Submission Deadline": release.get("tender", {}).get("tenderPeriod", {}).get("endDate", "N/A"),
                    "Procurement Method": release.get("tender", {}).get("procurementMethodDetails", "N/A"),
                    # To check if always the case. What if no bids for example
                    "Number of Tenders received": next(
                        (stat.get("value", "N/A") 
                        for stat in release.get("bids", {}).get("statistics", [])
                        if stat.get("measure") == "bids"),
                        "N/A"
                    ),
                    "Number of Tenders assessed": next(
                        (stat.get("value", "N/A") 
                        for stat in release.get("bids", {}).get("statistics", [])
                        if stat.get("measure") == "finalStageBids"),
                        "N/A"
                    ),
                    "Award decision date": release.get("awards", [{}])[0].get("date", "N/A"),
                    "Date assessment summaries sent": release.get("awards", [{}])[0].get("assessmentSummariesDateSent", "N/A"),
                    "Contracting Authority": release.get("buyer", {}).get("name", "N/A"),
                    "PPON": release.get("buyer", {}).get("id", "N/A"),
                    "Contact Name": release.get("parties", [{}])[0].get("contactPoint", {}).get("name", "N/A"),
                    "Contact Email": release.get("parties", [{}])[0].get("contactPoint", {}).get("email", "N/A"),
                    "Days to Award": (int((pd.to_datetime(release.get("date", ""), errors='coerce', utc=True)
                    - pd.to_datetime(release.get("contracts", [{}])[0].get("dateSigned", ""), errors='coerce', utc=True)).total_seconds() // 86400) 
                    if release.get("contracts", [{}])[0].get("dateSigned") and release.get("date")
                    else ""),
                    }
                award_notice_results.append(notice_fields)

                # Check lots info for UK6 notices and data pull through
                if len(lots) > 1:  # Only create lot entries for multiple lots
                    for idx, lot in enumerate(lots, 1):
                        lot_fields = { 
                            "OCID": release.get("ocid", "N/A"),
                            "Notice Type": notice_type,
                            "Is Update": is_update,
                            "Lot Number": idx,
                            "Lot Title": lot.get("title", "N/A"),
                            "Lot Description": lot.get("description", "N/A"),
                            "Lot Value ex VAT": lot.get("value", {}).get("amount", "N/A"),
                            "Lot Value inc VAT": lot.get("value", {}).get("amountGross", "N/A"),
                            "Lot Currency": lot.get("value", {}).get("currency", "N/A"),
                            "Lot Start Date": lot.get("contractPeriod", {}).get("startDate", "N/A"),
                            "Lot End Date": lot.get("contractPeriod", {}).get("endDate", "N/A"),
                            "SME Suitable": lot.get("suitability", {}).get("sme", False),
                            "VCSE Suitable": lot.get("suitability", {}).get("vcse", False),
                            "Award Criteria": (
                                lot.get("awardCriteria", {}).get("description", "N/A")
                                if not lot.get("awardCriteria", {}).get("criteria")
                                else "Refer to notice for detailed weightings"
                                ),
                            "CPV Code": (
                                    next(
                                    (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                    for item in release.get("tender", {}).get("items", [])
                                    if item.get("relatedLot") == lot.get("id")),
                                    "N/A"
                                )
                            ),
                        }
                        lot_results.append(lot_fields)

                #Separate UK 6 notices out - fields differ from other awards
                if notice_type in ["UK6", "UK7"]:
                    awards = release.get("awards", [])
                    for award in awards:
                        award_fields = {
                            "OCID": release.get("ocid", "N/A"),
                            "Notice Type": notice_type,
                            "Notice ID": release.get("id", "N/A"),
                            "Published Date": release.get("date", "N/A"),
                            "Is Update": is_update,
                            "Contract Title": award.get("title", "N/A"),
                            # For UK7, try to get value from contract first, then fall back to award
                            "Value ex VAT": (
                                release.get("contracts", [{}])[0].get("value", {}).get("amount", "N/A") 
                                if notice_type == "UK7" 
                                else award.get("value", {}).get("amount", "N/A")
                            ),
                            "Value inc VAT": (
                                release.get("contracts", [{}])[0].get("value", {}).get("amountGross", "N/A")
                                if notice_type == "UK7"
                                else award.get("value", {}).get("amountGross", "N/A")
                            ),
                            "Currency": award.get("value", {}).get("currency", "N/A"),
                            "Suppliers": ", ".join([supplier.get("name", "N/A") for supplier in award.get("suppliers", [])]),
                            "Contract Start Date": (
                                release.get("contracts", [{}])[0].get("period", {}).get("startDate", "N/A")
                                if notice_type == "UK7"
                                else award.get("contractPeriod", {}).get("startDate", "N/A")
                            ),
                            "Contract End Date": (
                                release.get("contracts", [{}])[0].get("period", {}).get("endDate", "N/A")
                                if notice_type == "UK7"
                                else award.get("contractPeriod", {}).get("endDate", "N/A")
                            ),
                            "Main Category": award.get("mainProcurementCategory", release.get("tender", {}).get("mainProcurementCategory", "N/A")),
                            "CPV Code": next(
                                (item.get("additionalClassifications", [{}])[0].get("id", "N/A")
                                for item in award.get("items", [])
                                if item.get("additionalClassifications")),
                                "N/A"
                            )
                        }
                        award_results.append(award_fields)

        

        # Convert results to DataFrames
        planning_df = pd.DataFrame(planning_results)
        tender_df = pd.DataFrame(tender_results)
        award_df = pd.DataFrame(award_notice_results)
        lots_df = pd.DataFrame(lot_results)
        awards_df = pd.DataFrame(award_results)
        procurement_terminations_df = pd.DataFrame(procurement_termination_results)
        
        # Clean data - replace None, empty lists, and other problematic values
        def clean_value(val):
            if val is None:
                return ""
            if isinstance(val, (list, dict)):
                if not val:  # Empty list or dict
                    return ""
                return str(val)
            if isinstance(val, float):
                if not math.isfinite(val):  # Check for inf or nan
                    return ""
            return val

        # Clean DataFrames
        for df in [planning_df, tender_df, award_df, lots_df, awards_df, procurement_terminations_df]:
            if not df.empty:
                for col in df.columns:
                    df[col] = df[col].apply(clean_value)
                    df[col] = df[col].replace([np.inf, -np.inf, np.nan], '')

        

        # --- Begin closed unawarded logic ---
        closed_unawarded_df = pd.DataFrame()
        try:
            logger.info("Analyzing closed unawarded notices")

            if tender_df.empty:
                logger.info("No tender notices found")
            else:
                uk4_notices = tender_df[tender_df['Notice Type'] == 'UK4'].copy()
                if uk4_notices.empty:
                    logger.info("No UK4 notices found")
                else:
                    latest_uk4 = uk4_notices.sort_values('Published Date').groupby('OCID').last()
                    current_date = datetime.now(timezone.utc)
                    closed_tenders = latest_uk4[
                        pd.to_datetime(latest_uk4['Submission Deadline'], errors='coerce', utc=True) < current_date
                    ]
                    if not procurement_terminations_df.empty and 'OCID' in procurement_terminations_df.columns:
                        terminated_ocids = set(procurement_terminations_df['OCID'].dropna())
                        closed_tenders = closed_tenders[~closed_tenders.index.isin(terminated_ocids)]
                        logger.info(f"Excluded {len(closed_tenders[closed_tenders.index.isin(terminated_ocids)])} closed tenders from terminated procurements")
                    if closed_tenders.empty:
                        logger.info("No closed tenders found")
                    else:
                        if award_df.empty:
                            unawarded = closed_tenders
                            logger.info("No award notices found - treating all closed tenders as unawarded")
                        else:
                            awarded_ocids = set(award_df[award_df['Notice Type'].isin(['UK6', 'UK7'])]['OCID'])
                            unawarded = closed_tenders[~closed_tenders.index.isin(awarded_ocids)]
                        closed_unawarded = unawarded.reset_index()[
                            ['OCID', 'Notice Type', 'Notice Title', 'Submission Deadline', 'Published Date', 
                            'Value ex VAT', 'Contracting Authority', 'Contact Name', 'Contact Email']
                        ]
                        closed_unawarded['Date Added to Report'] = current_date.strftime("%Y-%m-%dT%H:%M:%S%z")
                        closed_unawarded['Days Since Closed'] = (
                            current_date - pd.to_datetime(closed_unawarded['Submission Deadline'], errors='coerce', utc=True)
                        ).dt.days
                        closed_unawarded['Status'] = closed_unawarded['Days Since Closed'].apply(
                            lambda x: "Recently Closed" if x <= 30 else "Overdue Award Notice"
                        )
                        closed_unawarded_df = closed_unawarded
        except Exception as e:
            logger.error(f"Error analyzing closed unawarded notices: {str(e)}")


        award_days = pd.to_numeric(award_df['Days to Award'], errors='coerce').dropna()
        award_bins = [0, 30, 60, np.inf]
        award_labels = ['0-30', '31-60', '61+']
        award_df['Days to Award Bin'] = pd.cut(award_days, bins=award_bins, labels=award_labels, right=True)
        award_summary = award_df['Days to Award Bin'].value_counts().reindex(award_labels, fill_value=0).reset_index()
        award_summary.columns = ['Range', 'Award Count']

        # Days Since Closed summary (ignore blanks/non-numeric)
        closed_days = pd.to_numeric(closed_unawarded_df['Days Since Closed'], errors='coerce').dropna() if not closed_unawarded_df.empty else pd.Series(dtype=float)
        closed_bins = [0, 30, 60, np.inf]
        closed_labels = ['0-30', '31-60', '61+']
        if not closed_unawarded_df.empty:
            closed_unawarded_df['Days Since Closed Bin'] = pd.cut(closed_days, bins=closed_bins, labels=closed_labels, right=True)
            closed_summary = closed_unawarded_df['Days Since Closed Bin'].value_counts().reindex(closed_labels, fill_value=0).reset_index()
            closed_summary.columns = ['Range', 'Closed Unawarded Count']
        else:
            closed_summary = pd.DataFrame({'Range': closed_labels, 'Closed Unawarded Count': [0, 0, 0]})
        #Merge summaries for output
        summary = pd.merge(award_summary, closed_summary, on='Range', how='outer')

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if not planning_df.empty:
                planning_df.to_excel(writer, sheet_name='Planning_Notices', index=False)
            if not tender_df.empty:
                tender_df.to_excel(writer, sheet_name='Tender_Notices', index=False)
            if not award_df.empty:
                award_df.to_excel(writer, sheet_name='Award_Notices', index=False)
            if not lots_df.empty:
                lots_df.to_excel(writer, sheet_name='Lots', index=False)
            if not awards_df.empty:
                awards_df.to_excel(writer, sheet_name='Awards', index=False)
            if not procurement_terminations_df.empty:
                procurement_terminations_df.to_excel(writer, sheet_name='Procurement_Terminations', index=False)
            if not closed_unawarded_df.empty:
                closed_unawarded_df.to_excel(writer, sheet_name='Closed_Unawarded_Notices', index=False)
            if not summary.empty:
                summary.to_excel(writer, sheet_name='Days_to_Award_Summary', index=False)
                workbook = writer.book
                worksheet = writer.sheets['Days_to_Award_Summary']

                # Bar chart for Days to Award
                max_y = max(1, int(summary[['Award Count', 'Closed Unawarded Count']].max().max()))
                chart1 = workbook.add_chart({'type': 'column'})
                chart1.add_series({
                    'name': '0-30 days',
                    'categories': ['Days_to_Award_Summary', 1, 0, 1, 0],  # Just first category
                    'values': ['Days_to_Award_Summary', 1, 1, 1, 1],     # Just first value
                    'fill': {'color': '#3498db'},  
                    'data_labels': {'value': True},
                })

                chart1.add_series({
                    'name': '31-60 days',
                    'categories': ['Days_to_Award_Summary', 2, 0, 2, 0],  # Second category
                    'values': ['Days_to_Award_Summary', 2, 1, 2, 1],     # Second value
                    'fill': {'color': '#2ecc71'}, 
                    'data_labels': {'value': True}, 
                })

                chart1.add_series({
                    'name': '61-90 days',
                    'categories': ['Days_to_Award_Summary', 3, 0, 3, 0],  # Third category
                    'values': ['Days_to_Award_Summary', 3, 1, 3, 1],     # Third value
                    'fill': {'color': '#f39c12'},
                    'data_labels': {'value': True}, 
                })

                chart1.add_series({
                    'name': '90+ days',
                    'categories': ['Days_to_Award_Summary', 4, 0, 4, 0],  # Fourth category
                    'values': ['Days_to_Award_Summary', 4, 1, 4, 1],     # Fourth value
                    'fill': {'color': '#e74c3c'},
                    'data_labels': {'value': True},  
                })
                chart1.set_title({'name': 'Days to Award Distribution'})
                chart1.set_x_axis({'name': 'Days to Award Range'})
                chart1.set_y_axis({
                    'name': 'Count',
                    'major_unit': 1,
                    'minor_unit': 1,
                    'min': 0,
                    'max': max(1, int(summary[['Award Count', 'Closed Unawarded Count']].max().max()))
                })
                chart1.set_style(10)
                worksheet.insert_chart('E2', chart1)

                # Bar chart for Days Since Closed
                chart2 = workbook.add_chart({'type': 'column'})
                chart2.add_series({
                    'name': '0-30 days',
                    'categories': ['Days_to_Award_Summary', 1, 0, 1, 0],
                    'values': ['Days_to_Award_Summary', 1, 2, 1, 2],     # Column C values
                    'fill': {'color': '#3498db'},
                    'data_labels': {'value': True}, 
                })

                chart2.add_series({
                    'name': '31-60 days',
                    'categories': ['Days_to_Award_Summary', 2, 0, 2, 0],
                    'values': ['Days_to_Award_Summary', 2, 2, 2, 2],
                    'fill': {'color': '#2ecc71'},
                    'data_labels': {'value': True}, 
                })

                chart2.add_series({
                    'name': '61-90 days',
                    'categories': ['Days_to_Award_Summary', 3, 0, 3, 0],
                    'values': ['Days_to_Award_Summary', 3, 2, 3, 2],
                    'fill': {'color': '#f39c12'},
                    'data_labels': {'value': True}, 
                })

                chart2.add_series({
                    'name': '90+ days',
                    'categories': ['Days_to_Award_Summary', 4, 0, 4, 0],
                    'values': ['Days_to_Award_Summary', 4, 2, 4, 2],
                    'fill': {'color': '#e74c3c'},
                    'data_labels': {'value': True}, 
                })
                chart2.set_title({'name': 'Days Since Closed (Unawarded)'})
                chart2.set_x_axis({'name': 'Days Since Closed Range'})
                chart2.set_y_axis({
                    'name': 'Count',
                    'major_unit': 1,
                    'minor_unit': 1,
                    'min': 0,
                    'max': max_y
                })
                chart2.set_style(10)
                worksheet.insert_chart('E18', chart2)
        output.seek(0)
        latest_report_bytes = output.getvalue()

        current_time = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%dT%H:%M:%S")

        return True, f"Data successfully processed at {last_run_time}"

    
        

    except Exception as e:
        logger.error(f"Error in fetch_and_process_data: {str(e)}")
        return False, f"Error processing data: {str(e)}"
    finally:
        job_running = False
 

# Route for manual triggering of the data fetch
@app.route('/run')
def run_job():
    global job_running, last_run_time, latest_report_bytes
    
    if job_running:
        return jsonify({
            "status": "in_progress",
            "message": "A job is already running, please try again later."
        })
    
    from_date = request.args.get('from_date') or '2025-02-24T00:00:00'
    to_date = request.args.get('to_date') or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    PPON = request.args.get('ppon')

    if not PPON:
        return jsonify({
            "status": "error",
            "message": "PPON (organisation ID) is required. Please provide a PPON value."
        }), 400

    def job():
        global latest_report_bytes, job_running, last_run_time
        job_running = True
        try:
            fetch_and_process_data(from_date, to_date, PPON)
            last_run_time = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%dT%H:%M:%S")
        finally:
            job_running = False


    # Run in a separate thread to not block the response
    thread = Thread(target=job)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Data fetch job has been started. Check logs for results.",
        "last_completed_run": last_run_time
    })

# Health check endpoint
@app.route('/')
def health_check():
    global last_run_time
    return jsonify({
        "status": "healthy",
        "service": "find-a-tender-data-fetcher",
        "job_running": job_running,
        "last_run": last_run_time
    })

@app.route('/update-closed')
def update_closed_notices():
    thread = Thread(target=update_closed_unawarded_notices)
    thread.start()
    return jsonify({
        "status": "started",
        "message": "Started analyzing closed unawarded notices"
    })


@app.route('/download-report')
def download_report():
    global latest_report_bytes
    if latest_report_bytes is None:
        return "No report available. Please run the job first.", 404
    return send_file(
        BytesIO(latest_report_bytes),
        download_name=f"find_a_tender_report_{datetime.now(ZoneInfo('Europe/London')).strftime('%Y%m%d_%H%M%S')}.xlsx",
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/page')
def main_page():
    return render_template('index.html')

if __name__ == '__main__':
    try:
        # Get port from environment variable or use default 5000
        port = int(os.environ.get('PORT', 5000))
        
        # Add host='0.0.0.0' to make the server publicly accessible
        # Add debug=False for production
        app.run(
            host='0.0.0.0',  # Listen on all available interfaces
            port=port,
            debug=False      # Disable debug mode in production
        )
        
    except Exception as e:
        print(f"Failed to start server: {str(e)}")
        # Log the error and exit with non-zero status
        sys.exit(1)