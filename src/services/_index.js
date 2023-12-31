import models from '../models/_index.js';
import FlowService from './flow.js';
import PriorityService from './priority.js';
import FlowStageService from './flowStage.js';
import FlowUserService from './flowUser.js';
import ProcessService from './process.js';
import StageService from './stage.js';
import StatisticsService from './statistics.js';
import ProcessAud from './processAud.js';
import { UnitService } from './unit.js';
import { ProcessesFileService } from './processesFile.js';
import { UserService } from './user.js';
import UserAccessLogService from './userAccessLog.js';

const flowService = new FlowService(models.Flow);
const priorityService = new PriorityService(models.Priority);
const flowStageService = new FlowStageService(models.FlowStage);
const flowUserService = new FlowUserService(models.FlowUser);
const processService = new ProcessService(models.Process);
const processAudService = new ProcessAud(models.ProcessAud);
const stageService = new StageService(models.Stage);
const statisticsService = new StatisticsService();
const unitService = new UnitService(models.Unit);
const processesFileService = new ProcessesFileService(models.ProcessesFile);
const userService = new UserService();
const userAccessLogService = new UserAccessLogService();

const services = {
  flowService,
  priorityService,
  flowStageService,
  flowUserService,
  processService,
  processAudService,
  stageService,
  statisticsService,
  unitService,
  processesFileService,
  userService,
  userAccessLogService,
};

export default services;
