import { DataSource, DataSourceConfig } from 'apollo-datasource';
import { ApolloError } from 'apollo-server-errors';
import { S3 } from 'aws-sdk';
import { S3Cache } from './';

export abstract class S3DataSource<TContext = any> extends DataSource {
  cache!: S3Cache;

  constructor(protected s3: S3 = new S3()) {
    super();
  }

  initialize(config: DataSourceConfig<TContext>): void {
    this.cache = new S3Cache(config.cache, { region: this.s3.config.region });
  }

  protected async getObject(params: S3.Types.GetObjectRequest): Promise<S3.Types.GetObjectOutput['Body']> {
    try {
      const cachedOutput = await this.cache.get(params);

      if (cachedOutput) {
        return cachedOutput;
      } else {
        const output = await this.s3.getObject(params).promise();
        if (!output.Body) {
          return undefined;
        }
        await this.cache.setObject(params, output);
        return output.Body;
      }
    } catch (error) {
      throw new ApolloError(error.message, 'S3_DATA_SOURCE', { error, params });
    }
  }

  protected async deleteObject(params: S3.Types.DeleteObjectRequest): Promise<void> {
    try {
      await this.s3.deleteObject(params).promise();
      await this.cache.deleteObject(params);
    } catch (error) {
      throw new ApolloError(error.message, 'S3_DATA_SOURCE', { error, params });
    }
  }

  protected async listObjects(params: S3.Types.ListObjectsRequest): Promise<S3.Types.ObjectList> {
    try {
      const { Contents } = await await this.s3.listObjects(params).promise();
      return Contents;
    } catch (error) {
      throw new ApolloError(error.message, 'S3_DATA_SOURCE', { error, params });
    }
  }

  protected async putObject(params: S3.Types.PutObjectRequest): Promise<void> {
    try {
      await this.s3.putObject(params).promise();
      await this.cache.deleteObject(params);
    } catch (error) {
      throw new ApolloError(error.message, 'S3_DATA_SOURCE', { error, params });
    }
  }

  protected async getObjectJson<TResult = any>(params: S3.Types.GetObjectRequest): Promise<TResult> {
    const body = await this.getObject(params);
    try {
      return JSON.parse(String(body));
    } catch (error) {
      throw new ApolloError('Missing or invalid body', 'S3_DATA_SOURCE', { error, params });
    }
  }

  protected async putObjectJson<TObject = any>(object: TObject, params: S3.Types.PutObjectRequest): Promise<void> {
    await this.putObject({ ...params, ContentType: 'application/json', Body: JSON.stringify(object) });
  }
}
