
interface ITopicInfo {
    topicId: string;
    messageId: number;
    savedMessageId: number;
    lastSaveChunk: number;
    lastSaveDur: string;
    lastSaveMoment: string;
    loadedPages: ILoadedPage[];
    activePages: number[];
    queues: ITopicQueue[];
    queueSize: number;
}

interface ILoadedPage {
    pageId: number,
    hasSkipped: boolean,
    percent: number,
    count: number,
    writePosition: number;
    subPages: number[];
    toSave: number;
}

interface ITopicQueue {
    queueId: string;
    ranges: IQueueRange[];
}

interface IQueueRange {
    fromId: number;
    toId: number;
}

interface IPersistentOperation {
    name: string;
    topicId: string;
    pageId: number,
    dur: string
}

interface IStatus {
    topics: ITopicInfo[];
    awaitingOperations: IPersistentOperation[];
    queuesSnapshotId: number;
    activeOperations: IPersistentOperation[];
    system: ISystemStatus,
    initialing: boolean
}


interface ISystemStatus {
    usedmem: number,
    totalmem: number
}