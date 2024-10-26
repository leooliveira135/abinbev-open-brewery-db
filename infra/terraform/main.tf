terraform {
	required_providers {
		aws = {
			source  = "hashicorp/aws"
			version = "~> 5.0"
		}
	}
}

provider "aws" {
	region = "us-east-1"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "abinbev-open-brewery-bronze-datalake"

  tags = {
    Name = "abinbev-open-brewery-bronze-datalake"
    Env = "dev"
    Tier = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "abinbev-open-brewery-silver-datalake"

  tags = {
    Name = "abinbev-open-brewery-silver-datalake"
    Env = "dev"
    Tier = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "abinbev-open-brewery-gold-datalake"

  tags = {
    Name = "abinbev-open-brewery-gold-datalake"
    Env = "dev"
    Tier = "gold"
  }
}