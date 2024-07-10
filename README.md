# Image Processing System

## Objective
This project aims to build a system to efficiently process image data from CSV files. The system will:
1. Receive a CSV file containing serial numbers, product names, and input image URLs.
2. Validate the CSV data format.
3. Asynchronously process the images by compressing them to 50% of their original quality.
4. Store the processed image data and associated product information in a MongoDB database.
5. Provide APIs to upload the CSV file, check the processing status, and get the processed images.

## Features
- Asynchronous API endpoints.
- Background processing of images.
- Integration with AWS S3 for storing processed images.
- Status tracking of image processing requests.
- Webhook flow to trigger an endpoint after processing images (optional).

## Technologies Used
- Python
- FastAPI
- MongoDB
- Asynchronous operations with `aiohttp` and `aiofiles`
- Image processing with Pillow (`PIL`)
- AWS S3 for storage
- Docker (for MongoDB)

## Setup

### Prerequisites
- Python 3.8+
- MongoDB
- AWS S3 account
- Docker (optional, for running MongoDB locally)

### Environment Variables
Create a `.env` file in the root directory of the project and add the following environment variables:

