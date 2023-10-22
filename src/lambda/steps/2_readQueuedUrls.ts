// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import { STATE_MACHINE_URL_THRESHOLD } from '../config/constants';
import { CrawlContext } from '../crawler/types';
import { readBatchOfUrlsToVisit } from '../utils/contextTable';
import { getHistoryEntry, putHistoryEntry } from '../utils/historyTable';

import * as AWS from 'aws-sdk';
const s3 = new AWS.S3();

/**
 * Read all non visited urls from the context database so that they can be distributed to the sync lambdas
 */
export const readQueuedUrls = async (crawlContext: CrawlContext) => {
  const historyEntry = await getHistoryEntry(crawlContext.crawlId);

  const { urlCount, batchUrlCount } = historyEntry;

  // Get a batch of urls we haven't visited yet
  const urlsToVisit = await readBatchOfUrlsToVisit(crawlContext.contextTableName);

  console.log('urlsToVisit.length = ', urlsToVisit.length);
  console.log('Urls to visit', urlsToVisit);

  const totalUrlCount = urlCount + urlsToVisit.length;
  const totalBatchUrlCount = batchUrlCount + urlsToVisit.length;
  console.log('Total urls:', totalUrlCount);
  console.log('Total urls in current step function execution:', totalBatchUrlCount);
  console.log('Total urls exceeding threshold:', totalBatchUrlCount > STATE_MACHINE_URL_THRESHOLD);

  // Write the total urls back to the history table
  await putHistoryEntry({
    ...historyEntry,
    urlCount: totalUrlCount,
    batchUrlCount: totalBatchUrlCount,
  });

  let queuedPaths = urlsToVisit.map((path) => ({
    path,
    crawlContext
  }))

  // Save queuedPaths to S3
  const bucket = "tfc-data";
  const key = `${crawlContext.crawlId}.queuedPaths.json`;
  let res = await s3.putObject({ Bucket: "tfc-data", Key: key, Body: JSON.stringify(queuedPaths) }).promise();
  console.log('queuedPaths saved to S3.', res);
  
  return {
    totalUrlCountExceedsThreshold: totalBatchUrlCount > STATE_MACHINE_URL_THRESHOLD,
    queueIsNonEmpty: urlsToVisit.length > 0,
    crawlContext,
    queuedPaths: {
      s3: {
        bucket,
        key
      }
    } 
  };

  /*
  return {
    totalUrlCountExceedsThreshold: totalBatchUrlCount > STATE_MACHINE_URL_THRESHOLD,
    queueIsNonEmpty: urlsToVisit.length > 0,
    queuedPaths: urlsToVisit.map((path) => ({
      path,
      crawlContext,
    })),
    crawlContext,
  };
  */
};
