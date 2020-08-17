---


---

<h1 id="project--data-pipelines-with-airflow">Project:  Data Pipelines with Airflow</h1>
<h2 id="introduction">Introduction</h2>
<p>A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.</p>
<p>The expected delivarables are to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.</p>
<p>he source data resides in S3 and needs to be processed in Sparkify’s data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.</p>
<h2 id="data-sets">Data Sets</h2>
<p>For this project, you’ll be working with two datasets. Here are the s3 links for each:</p>
<ul>
<li>Log data:  <code>s3://udacity-dend/log_data</code></li>
<li>Song data:  <code>s3://udacity-dend/song_data</code></li>
</ul>
<h2 id="project-template"># Project Template</h2>
<p>The project consists of three major components:</p>
<ul>
<li>
<p>The  <strong>dag template</strong>  has all the imports and task dependencies.</p>
</li>
<li>
<p>The  <strong>operators</strong>  folder with operator templates:</p>
<ul>
<li>
<p>The <strong>stage operator</strong> loads any JSON<br>
formatted files from S3 to Amazon Redshift. The operator creates and<br>
runs a SQL COPY statement based on the parameters provided. The<br>
operator’s parameters should specify where in S3 the file is loaded<br>
and what is the target table.</p>
</li>
<li>
<p>The <strong>dimension and fact operators</strong> utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against.</p>
</li>
<li>
<p>The final operator is the <strong>data quality operator</strong>, which is used to run checks on the data itself. The operator’s main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.</p>
</li>
</ul>
</li>
<li>
<p>A  <strong>helper class</strong>  for the SQL transformations</p>
</li>
</ul>
<p>Dependencies are set so the graph view follows the flow shown in the image below.<br>
<img src="https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png" alt="enter image description here"></p>
<h2 id="airflow-connections">Airflow Connections</h2>
<p>For AWS credentials, enter the following values:</p>
<ul>
<li><strong>Conn Id</strong>: Enter  <code>aws_credentials</code>.</li>
<li><strong>Conn Type</strong>: Enter  <code>Amazon Web Services</code>.</li>
<li><strong>Login</strong>: Enter your  <strong>Access key ID</strong>  from the IAM User credentials you downloaded earlier.</li>
<li><strong>Password</strong>: Enter your  <strong>Secret access key</strong>  from the IAM User credentials you downloaded earlier.</li>
</ul>
<p>Use the following values in Airflow’s UI to configure connection to Redshift:</p>
<ul>
<li><strong>Conn Id</strong>: Enter  <code>redshift</code>.</li>
<li><strong>Conn Type</strong>: Enter  <code>Postgres</code>.</li>
<li><strong>Host</strong>: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the  <strong>Clusters</strong>  page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to  <strong>NOT</strong>  include the port at the end of the Redshift endpoint string.</li>
<li><strong>Schema</strong>: Enter  <code>dev</code>. This is the Redshift database you want to connect to.</li>
<li><strong>Login</strong>: Enter  <code>awsuser</code>.</li>
<li><strong>Password</strong>: Enter the password you created when launching your Redshift cluster.</li>
<li><strong>Port</strong>: Enter  <code>5439</code>.</li>
</ul>
<h2 id="instructions">Instructions</h2>
<ul>
<li>Run <code>/opt/airflow.start.sh</code> to start the Airflow webserver.</li>
<li>Once the Airflow web server is ready, you can access the Airflow UI by clicking on the blue <code>Access Airflow</code> button.</li>
</ul>
<p>This project is completed as a part of  <a href="https://www.udacity.com/course/data-engineer-nanodegree--nd027?utm_source=gsem_brand&amp;utm_medium=ads_r&amp;utm_campaign=8826748985_c&amp;utm_term=88603514323&amp;utm_keyword=udacity%20data%20engineer_e&amp;gclid=CjwKCAjw1ej5BRBhEiwAfHyh1LJE9bbir4kCyJjj0cAdE5HBb9F9YOxcXwrQNZLz_ieHirhgGkPd8xoC7tAQAvD_BwE">Udacity Data Engineer Nanodegree</a> program.</p>
<blockquote>
<p>Written with <a href="https://stackedit.io/">StackEdit</a>.</p>
</blockquote>

