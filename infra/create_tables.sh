#!/bin/bash
REGION="ap-south-1"

# Edit volume metrics (edits/min by wiki)
aws dynamodb create-table \
    --table-name wiki-edit-metrics \
    --attribute-definitions \
        AttributeName=window_end,AttributeType=S \
        AttributeName=wiki,AttributeType=S \
    --key-schema \
        AttributeName=wiki,KeyType=HASH \
        AttributeName=window_end,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

# Anomalies (pages with >20 edits/min)
aws dynamodb create-table \
    --table-name wiki-anomalies \
    --attribute-definitions \
        AttributeName=window_end,AttributeType=S \
        AttributeName=page_title,AttributeType=S \
    --key-schema \
        AttributeName=window_end,KeyType=HASH \
        AttributeName=page_title,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

# Top edited pages per window
aws dynamodb create-table \
    --table-name wiki-top-pages \
    --attribute-definitions \
        AttributeName=window_end,AttributeType=S \
        AttributeName=page_title,AttributeType=S \
    --key-schema \
        AttributeName=window_end,KeyType=HASH \
        AttributeName=page_title,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

echo "Tables created."