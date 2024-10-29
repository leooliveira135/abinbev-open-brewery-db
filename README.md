# abinbev-open-brewery-db

Here's the README file for the **abinbev-open-brewery-db** project that shows how to deploy the infrastructure created to build the datalake layers and the tools used during the development.

# Tools

The development was made into the **Fedora Linux** distribution, if you're using **another OS** check the docs for each tool and follow the instructions regarding your environment.

So, here are the tools used for the development of this project:

 1. [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
 2. [AWS Cli](https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html)
 3. [Docker](https://docs.docker.com/desktop/install/linux/)
 4. [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

Also, I used AWS as the main cloud provider to create the datalake layers, store the data and query the results. The main services used in the development process were:

 1. [S3](https://aws.amazon.com/pt/s3/)
 2. [Glue](https://aws.amazon.com/pt/glue/)
 3. [Athena](https://aws.amazon.com/pt/athena/)
 4. [IAM](https://aws.amazon.com/pt/iam/)

## Download development environment

Now we're going to install the tools listed in the step above. To do this, run the `setup.sh` file stored in the github project.

Run this command:

    sh <your_path>/setup.sh

## Setup AWS Cli
We'll use the AWS as our main cloud provider, to do so you'll need to have or create an account there, after you login I suggest you to create a new user for setting up the aws cli.

### AWS Cli User setup
First, go to the AWS console in your browser and click on the search bar and insert **IAM** on it.

Then, you go to the left menu and click on **Users**, select an existing user or create a new one, then click on the user name and go to the **Security credentials** tab, after you'll go to the **Access Key** part and click on the **Create access key** button. 

Now, you're going to start the access key creation, first select the **Command Line Interface (CLI)** option, click on the confirmation check box and finally click on the next button. After, **give a name to this credential** and click next again.

**IMPORTANT:** The credentials are created successfully, but before clicking on the Done button, **YOU MUST DOWNLOAD the csv file and store it in a safe place**, to do so you'll click on the **Download .csv file**. After the download is completed, then click on Done.

### AWS Cli Command line setup
With the needed credentials created, now we're going to setup the aws cli user in the linux command line.

By doing so, you'll have to run the following commands:

 1. First check if the aws cli is installed, by running the `aws --version` command.
 2. Now you're starting the aws cli user configuration, by running the `aws configure` command. After running the command, you'll see these questions and will fill with the following input:

> AWS Access Key ID [None]: **aws-access-key** (this information is in the csv file that you've downloaded earlier)
> AWS Secret Access Key [None]: **aws-access-secret-key** (this information is in the csv file that you've downloaded earlier)
> Default region name [None]: **us-east-1**
> Default output format [None]: **json**

3. And to check if the configuration made is correct, run this command `aws sts get-caller-identity` and if it returns your credentials than you're able to use AWS in your local environment

## Create S3 buckets with terraform

Now, we're going to create the S3 buckets for all the datalake layers, and besides doing this by going to the AWS console in your browser and create them there, we'll do it with terraform.

In the github project, enter the `infra/terraform` directory and there you'll see a file called `main.tf` . To create the buckets you'll run the following commands:

    terraform init
    terraform plan

Important to check if the terraform plan command result returns without error, just showing that you're going to implement in the environment will be applied. To do that, you have to run this command:

    terraform apply

If everything goes well, then you can go to AWS and enter the S3 main page to see the new buckets created with terraform.

## Running airflow with docker

In the `setup.sh` file we're installing docker into our environment and after that downloading an airflow official docker-compose file. But I did a change that is real important, that we're adding jdk into the official airflow image. 

So, in the env file you'll have a variable called **AIRFLOW_IMAGE_NAME**, I suggest you to use it in the docker compose file. 

Go to the docker compose file and do the following:

    In line #52 put this value in image variable: ${AIRFLOW_IMAGE_NAME} 
    And uncomment the line below:  build: .

After that run `docker compose up` and wait a while, then go to your browser and reach http://localhost:8080


# Airflow dag
We're using Airflow as our orchestration tool and there that our pipeline will be available to load data into the datalake layers. The dag is called as "**Fetch_Open_Brewery_Data**" and its scheduled to run every day at 04:05 AM.

But how can Airflow connects with AWS? In the menu located in your top screen, you'll see the **Admin** tab, click on the **Connections** link. Then you click on the **+ blue button** to create a new connection.

Now you'll see a form and fill it with the regarding information:

 - **Connection Id** airflow-aws
 - **Connection Type** Amazon Web Services
 - **AWS Access Key ID** aws-access-key (this information is in the csv file that you've downloaded earlier in the aws cli setup)
 - **AWS Secret Access Key** aws-secret-access-key(this information is in the csv file that you've downloaded earlier in the aws cli setup)
 - **Extra** {“region”:”us-east-1”}

With all set, click on **Save** and that's it.

### Explaining the airflow tasks

In this dag, you'll see three tasks, one for every datalake layer, the first task called `load_bronze_data()` that loads the raw data into the bronze layer S3 bucket, and also loads the `requirements.txt` file with the python dependencies used in the project.

After that, you'll see the `load_silver_data()` task that will do the ETL process of the raw data and store it in the silver datalake layer with the delta lake paradigm partitioned by country and province. Finally, the `load_gold_data()` task will load the aggregated data into the golden datalake layer with the delta lake paradigm making the data available for visualization tools and machine learning.

In the `dag.py` file the airflow operator used to create the tasks is the `PythonVirtualenvOperator`, that uses Python as the bash inside a virtual environment, like Anaconda or Pyenv.

## Glue

For the data be available on Athena in AWS, we will use Glue as our data catalog, for that to work we need to create some databases and crawlers in glue.

### Create a glue database
In the AWS console in your browser, reach Glue in the search bar. After that, in your left side there's a menu, go to the **Data Catalog** tab and click on databases. In the databases main page, click on **Add database** button.

We're going to create two databases, one for the silver datalake layer and another for the gold datalake layer, they will be used in Athena as a schema for the tables stored there. To create them, do the following steps:

Insert a name, I recommend you to use the same name from the datalake bucket, replacing the hyphens (-) **for underscores (_)** and in the database settings insert the S3 URI from the corresponding datalake bucket. With all set, click on the **Create database** button and the glue database is created successfully.

### Create a glue crawler
In the AWS console in your browser, reach Glue in the search bar. After that, in your left side there's a menu, go to the **Data Catalog** tab and click on crawlers. In the crawlers main page, click on **Create crawler** button.

A crawler will search for the data stored in a S3 bucket and create all the tables available there. So a crawler needs a glue database where the tables created by it can be accessible in Athena.

Now, to create a crawler follow the steps below:

 1. Give the crawler a name, then click on next.
 2. In the data sources section, select the **Not Yet** option first and then click on the **Add data sources** button.
 3. A pop up window will be open with the data source configuration, select the **Delta Lake source** and insert the S3 table paths where the data is stored, then click on **Add a Delta Lake data source** button.
 4. Then you'll need to set up a IAM Role for the crawler, if you don't have any created, just click on the **Create new IAM role** button and insert a name for it, then click on create and after that click on next buttons. **IMPORTANT:** Go to IAM in AWS and open the role that you already created and check if the role has the right permissions, that are: **AmazonS3FullAccess** and **AWSGlueServiceRole**, if there are only one or none of them, add them into the role used in the glue crawler.
 5. In the output configuration, select the database you've created before in the target database field. After that, in crawler schedule, select the **On Demand** option, with all set, click on next.
 6. With all this done, you'll see a overview showing the complete configuration with the crawler you're creating. If it's all correct, click on **Create crawler** button and the crawler is created successfully.
