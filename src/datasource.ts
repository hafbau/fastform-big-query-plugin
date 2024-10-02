import { IntegrationBase } from "@budibase/types";
import { GoogleAuth } from 'google-auth-library';
import { BigQuery } from '@google-cloud/bigquery';

interface Query {
  statement: string
}

class BigQueryIntegration implements IntegrationBase {
  private readonly base64EncodedServiceAccount: string;
  private readonly gcpProjectId: string;
  private authClient: GoogleAuth;

  constructor(config: { 
      base64EncodedServiceAccount: string,
      gcpProjectId: string,
    }
    ) {
    this.base64EncodedServiceAccount = config.base64EncodedServiceAccount;
    this.gcpProjectId = config.gcpProjectId;
  }

  private async getAuthClient() {
    if (this.authClient) return this.authClient;
    // https://github.com/orgs/vercel/discussions/219#discussioncomment-128702
    const serviceAccount = JSON.parse(
      Buffer.from(this.base64EncodedServiceAccount, "base64").toString().replace(/\n/g,"")
    )
    const auth = new GoogleAuth({
      credentials: serviceAccount,
      scopes: ['https://www.googleapis.com/auth/cloud-platform'],
    });
  
    this.authClient = await auth.getClient();
    return this.authClient;
  }
  async getClient() {
    const authClient = await this.getAuthClient();
    const bigqueryClient = new BigQuery({
      projectId: this.gcpProjectId,
      authClient,
    });

    return bigqueryClient;
  }

  async executeQuery(query: {sql: string }) {
    const bigqueryClient = await this.getClient();
    const options = {
      query: query.sql,
      // location: this.bqDatasetLocation,
    };
    
    let [job] = await bigqueryClient.createQueryJob(options);

    const [rows] = await job.getQueryResults();
    return rows;
  }

  async executeCommand(query: { sql: string }) {
    const bigqueryClient = await this.getClient();
    const options = {
      query: query.sql,
      // location: this.bqDatasetLocation,
    };
    
    let [job] = await bigqueryClient.createQueryJob(options);
    return [];
  }

  async create(query: { sql: string }) {
    return await this.executeCommand(query);
  }

  async read(query: { sql: string }) {
    return await this.executeQuery(query);
  }

  async update(query: { sql: string }) {
    return await this.executeCommand(query);
  }

  async delete(query: { sql: string }) {
    return await this.executeCommand(query);
  }
}

export default BigQueryIntegration
