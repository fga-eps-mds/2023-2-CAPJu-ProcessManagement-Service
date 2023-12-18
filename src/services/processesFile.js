import { literal, Op } from 'sequelize';
import xlsx from 'node-xlsx';
import XLSX from 'xlsx-js-style';
import models from '../models/_index.js';
import ProcessService from './process.js';
import FlowService from './flow.js';
import PriorityService from './priority.js';
import { promises as fs } from 'fs';
import path from 'path';
import { convertCsvToXlsx } from '@aternus/csv-to-xlsx';
import { logger } from '../utils/logger.js';
import sequelizeConfig from '../config/sequelize.js';
import { v4 as uuidv4 } from 'uuid';
import { formatDateTimeToBrazilian } from '../utils/date.js';

const validProcessesHeader = [
  'Número processo',
  'Número do Processo',
  'Processos',
];
const validNicknamesHeader = ['Apelido', 'Apelidos'];
const validFlowsHeader = ['Fluxo', 'Fluxos'];
const validPrioritiesHeaders = ['Prioridade', 'Prioridades', 'prioridades'];

export class ProcessesFileService {
  constructor(ProcessesFileModel) {
    this.repository = ProcessesFileModel;
    this.processService = new ProcessService(models.Process);
    this.flowService = new FlowService(models.Flow);
    this.priorityService = new PriorityService(models.Priority);
    this.processesFileItemRepository = models.ProcessesFileItem;
  }

  findAllPaged = async req => {
    const { offset = 0, limit = 10 } = req.query;

    const where = this.buildFileFilters(req);

    let include;

    if (req.query.nameOrRecord) {
      include = [
        {
          model: models.ProcessesFileItem,
          as: 'fileItems',
          attributes: [],
          where: {
            record: { [Op.iLike]: `%${req.query.nameOrRecord}%` },
          },
          required: false,
          duplicating: false,
        },
      ];
    }

    const data = await this.repository.findAll({
      where,
      offset,
      limit,
      order: [['idProcessesFile', 'DESC']],
      include,
      attributes: [
        'idProcessesFile',
        'name',
        'fileName',
        'status',
        'message',
        'createdAt',
        [
          literal(`(
        SELECT CAST(COUNT(*) AS INTEGER)
        FROM "processesFileItem"
        WHERE
          "processesFileItem"."idProcessesFile" = "ProcessesFileModel"."idProcessesFile"
      )`),
          'allItemsCount',
        ],
        [
          literal(`(
        SELECT CAST(COUNT(*) AS INTEGER)
        FROM "processesFileItem"
        WHERE
          "processesFileItem"."idProcessesFile" = "ProcessesFileModel"."idProcessesFile" AND
          "processesFileItem"."status" = 'error'
      )`),
          'errorItemsCount',
        ],
        [
          literal(`(
        SELECT CAST(COUNT(*) AS INTEGER)
        FROM "processesFileItem"
        WHERE
          "processesFileItem"."idProcessesFile" = "ProcessesFileModel"."idProcessesFile" AND
          "processesFileItem"."status" IN ('imported', 'manuallyImported')
      )`),
          'importedItemsCount',
        ],
      ],
      logging: false,
    });

    const totalCount = await this.repository.count({
      where,
      include,
    });
    const totalPages = Math.ceil(totalCount / limit);
    const currentPage = Math.floor(offset / limit) + 1;

    return {
      data,
      pagination: {
        totalRecords: totalCount,
        totalPages: totalPages,
        currentPage: currentPage,
        perPage: limit,
      },
    };
  };

  findById = async idProcessesFile => {
    return await this.repository.findOne({
      where: {
        idProcessesFile,
      },
      attributes: ['idProcessesFile', 'status', 'createdAt'],
      raw: true,
    });
  };

  findAllItemsPaged = async req => {
    const { offset = 0, limit = 10 } = req.query;

    const where = this.buildFileItemsFilters(req);

    const data = await this.processesFileItemRepository.findAll({
      where,
      offset,
      limit,
      order: [['status', 'DESC']],
      include: [
        {
          model: models.Process,
          as: 'generatedProcessInfo',
          attributes: ['idFlow'],
          required: false,
        },
      ],
      raw: true,
    });

    const totalCount = await this.processesFileItemRepository.count({ where });

    const totalPages = Math.ceil(totalCount / limit);
    const currentPage = Math.floor(offset / limit) + 1;

    return {
      data,
      pagination: {
        totalRecords: totalCount,
        totalPages: totalPages,
        currentPage: currentPage,
        perPage: limit,
      },
    };
  };

  createFile = async data => {
    return (({ idProcessesFile, status }) => ({
      idProcessesFile,
      status,
    }))(await this.repository.create(data));
  };

  updateFileItem = async (idProcessesFileItem, newData) => {
    return await this.processesFileItemRepository.update(newData, {
      where: { idProcessesFileItem },
      returning: false,
    });
  };

  findFileById = async (idProcessesFile, original, format) => {
    const fileKey = original ? 'dataOriginalFile' : 'dataResultingFile';

    const file = await this.repository.findOne({
      where: { idProcessesFile },
      attributes: ['idProcessesFile', fileKey, 'fileName'],
      raw: true,
    });

    // Assuming the original file will be stored in .xlsx
    if (!original && format === 'csv') {
      const buffer = file[fileKey];
      file[fileKey]['data'] = await this.convertXlsxToCsv(buffer);
      file[fileKey]['data'] = await this.convertXlsxToCsv(buffer);
    }

    return file;
  };

  deleteFileById = async idProcessesFile => {
    await this.processesFileItemRepository.destroy({
      where: { idProcessesFile },
    });
    return await this.repository.destroy({
      where: { idProcessesFile },
    });
  };

  generateResultingFile = async idProcessesFile => {
    const fileInfo = await this.repository.findOne({
      where: { idProcessesFile },
      attributes: ['name', 'importedAt'],
      raw: true,
    });
    const fileItems = await this.processesFileItemRepository.findAll({
      where: { idProcessesFile },
      attributes: {
        exclude: ['idProcessesFileItem', 'idProcessesFile', 'idProcess'],
      },
      raw: true,
    });

    let quantityWithError = 0;
    let quantityImported = 0;

    const statusStyleObj = {
      imported: 'IMPORTADO',
      error: 'ERRO',
      manuallyImported: 'IMPORTADO MANUALMENTE',
    };

    const fileItemsAsArrayOfArrays = fileItems.map(fileItem => {
      if (fileItem.status !== 'error') quantityImported++;
      else quantityWithError++;
      return [
        fileItem.record || '-',
        fileItem.nickname || '-',
        fileItem.flow || '-',
        fileItem.priority || '-',
        statusStyleObj[fileItem.status],
        fileItem.message || '-',
      ];
    });

    const resultingSheetData = [
      [
        {
          v: `RESULTADO IMPORTAÇÃO\n\nLote: ${
            fileInfo.name
          }\nData Importação: ${formatDateTimeToBrazilian(
            fileInfo.importedAt,
          )}\nImportados: ${quantityImported}\nErro: ${quantityWithError}`,
          t: 's',
          s: { alignment: { wrapText: true, horizontal: 'center' } },
        },
      ],
      [
        'Número do Processo',
        'Apelido',
        'Fluxo',
        'Prioridade',
        'Status',
        'Mensagens',
      ],
      ...fileItemsAsArrayOfArrays,
    ];

    const wb = XLSX.utils.book_new();

    const ws = XLSX.utils.aoa_to_sheet(resultingSheetData);

    ws['!merges'] = [{ s: { r: 0, c: 0 }, e: { r: 0, c: 5 } }];

    ws['!rows'] = [{ hpt: 90 }];

    fileItemsAsArrayOfArrays.forEach((fileItem, rowIndex) => {
      const messageCellRef = XLSX.utils.encode_cell({ r: rowIndex + 2, c: 5 });

      if (!ws[messageCellRef]) ws[messageCellRef] = {};
      ws[messageCellRef].v = fileItem[5].replace(/\\n/g, '\n');
      ws[messageCellRef].s = {
        alignment: { wrapText: true },
      };

      const statusCellRef = XLSX.utils.encode_cell({ r: rowIndex + 2, c: 4 });
      if (!ws[statusCellRef]) ws[statusCellRef] = {};
      ws[statusCellRef].s = {
        font: {
          color: {
            rgb: fileItem[4] === statusStyleObj.error ? 'd62d2d' : '34eb4c',
          },
        },
      };
    });

    const maxContentLengths =
      this.findMaxContentLengthPerColumn(resultingSheetData);

    console.log(maxContentLengths);

    ws['!cols'] = maxContentLengths.map(maxLength => ({
      wch: maxLength + 5,
    }));

    XLSX.utils.book_append_sheet(wb, ws, 'RESULTADO');

    const outputFile = XLSX.write(wb, {
      type: 'buffer',
      bookType: 'xlsx',
    });

    return outputFile;
  };

  imporFilesJob = async () => {
    // logic based on the assumption that the files will be small.
    const files = await this.repository.findAll({
      where: { status: 'waiting' },
      raw: true,
      order: [['idProcessesFile', 'ASC']],
      attributes: [
        'idProcessesFile',
        'fileName',
        'dataOriginalFile',
        'importedBy',
      ],
      limit: 10,
    });

    if (!files?.length) return;

    const idProcessesFile = files.map(f => f.idProcessesFile);

    await this.repository.update(
      { status: 'inProgress' },
      { where: { idProcessesFile }, returning: false },
    );

    for (const file of files) {
      try {
        let { dataOriginalFile, fileName } = file;

        if (this.isFileType(fileName, 'csv')) {
          dataOriginalFile = await this.convertCsvBufferToXlsx(
            dataOriginalFile,
            fileName,
          );
        }

        logger.info(
          `Iniciando processamento arquivo [${file.idProcessesFile}-${fileName}]`,
        );

        logger.info(
          `Iniciando parser arquivo [${file.idProcessesFile}-${fileName}]`,
        );

        const workbook = xlsx.parse(dataOriginalFile);

        logger.info(
          `Parser arquivo [${file.idProcessesFile}-${fileName}] concluído`,
        );

        const headerIndex = workbook[0].data.findIndex(row => row.length);

        if (headerIndex === -1) {
          throw new Error('Cabeçalho não encontrado');
        }

        const header = workbook[0].data[headerIndex];

        let headerErrorMessages = '';
        const validationsMandatoyHeaders = [
          {
            headers: validProcessesHeader,
            errorMessage: 'Coluna processos não encontrada',
            key: 'processHeaderIndex',
          },
          {
            headers: validFlowsHeader,
            errorMessage: 'Coluna fluxos não encontrada',
            key: 'flowsHeaderIndex',
          },
        ];

        const headerIndexes = {};
        validationsMandatoyHeaders.forEach(validation => {
          const index = header.findIndex(h => validation.headers.includes(h));
          if (index === -1) {
            headerErrorMessages = headerErrorMessages.concat(
              validation.errorMessage,
              '\n',
            );
          } else {
            headerIndexes[validation.key] = index;
          }
        });

        if (headerErrorMessages) {
          throw new Error(headerErrorMessages);
        }

        const prioritesIndex = header.findIndex(h =>
          validPrioritiesHeaders.includes(h),
        );
        if (prioritesIndex !== -1) {
          headerIndexes.prioritiesHeaderIndex = prioritesIndex;
        }

        for (const worksheet of workbook) {
          const sheetDataArray = worksheet.data.slice(headerIndex + 1);
          const sheetDataMap = new Map();
          Object.keys(headerIndexes).forEach(indexName => {
            const headerPosition = headerIndexes[indexName];
            sheetDataMap.set(
              headerPosition,
              sheetDataArray.map(a => a[headerPosition]),
            );
          });

          const numberOfRows = sheetDataMap.get(
            headerIndexes.processHeaderIndex,
          ).length;
          let processesFileItems = [];

          let rowIndex = 0;

          logger.info(
            `Populando mapa arquivo [${file.idProcessesFile}-${fileName}]`,
          );

          while (rowIndex < numberOfRows) {
            const record = sheetDataMap.get(headerIndexes.processHeaderIndex)[
              rowIndex
            ];
            const flow = sheetDataMap.get(headerIndexes.flowsHeaderIndex)[
              rowIndex
            ];
            const nickname = sheetDataMap
              .get(headerIndexes.nicknamesHeaderIndex)
              ?.at(rowIndex);
            const priority = sheetDataMap
              .get(headerIndexes.prioritiesHeaderIndex)
              ?.at(rowIndex);

            const fileItem = {
              record,
              flow,
              nickname,
              priority,
              idProcessesFile: file.idProcessesFile,
              rowIndex,
            };

            if (record || flow || nickname || priority) {
              processesFileItems.push(fileItem);
            }

            rowIndex++;
          }

          logger.info(`Mapa populado [${file.idProcessesFile}-${fileName}]`);

          const processes = [];

          const flows = await this.flowService.findAllRawWithAttributes(
            {
              name: Array.from(
                new Set(sheetDataMap.get(headerIndexes.flowsHeaderIndex)),
              ),
            },
            ['idFlow', 'idUnit', 'name'],
          );

          for (let j = 0; j < processesFileItems.length; j++) {
            const fileItem = processesFileItems[j];

            if (Object.values(fileItem).every(value => value === undefined))
              continue;

            let message = '';
            let status = 'imported';

            const process = {};

            const mandatoryEmptyFields = [
              { name: 'Número processo', key: 'record' },
              { name: 'Fluxo', key: 'flow' },
            ].filter(field => !fileItem[field.key]);

            if (mandatoryEmptyFields.length) {
              mandatoryEmptyFields.forEach(
                emptyField =>
                  (message = message.concat(`${emptyField.name} vazio \n`)),
              );
              status = 'error';
            }

            if (fileItem.flow) {
              const flow = flows.find(f => f.name === fileItem.flow);

              if (!flow) {
                message = message.concat(`Fluxo ${fileItem.flow} inválido \n`);
                status = 'error';
              } else {
                Object.assign(process, {
                  idFlow: flow.idFlow,
                  idUnit: flow.idUnit,
                });
              }
            }

            if (fileItem.record) {
              const { filteredRecord: record, valid } = this.validateRecord(
                fileItem.record,
              );

              if (!valid) {
                message = message.concat(
                  `Número de processo ${fileItem.record} fora do padrão CNJ \n`,
                );
                status = 'error';
              } else {
                Object.assign(process, { record });
              }
            }

            if (fileItem.nickname && fileItem.nickname.length > 50) {
              message = message.concat(
                `Apelido não pode exceder os 50 caracteres \n`,
              );
              status = 'error';
            }

            const idPriority = this.getPriorityIdByDescriptionOrAbbreviation(
              fileItem.priority,
            );
            if (idPriority === null) {
              message = message.concat(
                `Prioridade ${fileItem.priority} não encontrada \n`,
              );
              status = 'error';
            } else {
              Object.assign(process, { idPriority });
            }

            if (status !== 'error') {
              Object.assign(process, {
                nickname: fileItem.nickname,
                finalised: false,
              });
              fileItem.process = process;
              processes.push(process);
            } else {
              message = message.trim();
              Object.assign(fileItem, { message });
            }

            Object.assign(fileItem, { status });
          }

          const processesAuds = [];

          await sequelizeConfig.transaction(async transaction => {
            const processesResponse = await models.Process.bulkCreate(
              processes,
              { returning: true, logging: false, transaction },
            );

            processesResponse.forEach((process, i) => {
              process = process.toJSON();
              processes[i].idProcess = process.idProcess;
              processesAuds.push({
                processRecord: process.record,
                idProcess: process.idProcess,
                operation: 'INSERT',
                changedBy: file.importedBy,
                newValues: JSON.stringify(process),
                changedAt: new Date(),
                remarks: null,
                oldValues: null,
              });
            });

            processesFileItems = processesFileItems.map(fI => ({
              ...fI,
              idProcess: fI.process?.idProcess,
            }));

            await models.ProcessAud.bulkCreate(processesAuds, {
              transaction,
              returning: false,
              logging: false,
            });
            await this.processesFileItemRepository.bulkCreate(
              processesFileItems,
              { transaction, returning: false, logging: false },
            );
            await this.repository.update(
              { status: 'imported', message: null, importedAt: new Date() },
              { where: { idProcessesFile: file.idProcessesFile } },
              { transaction, returning: false, logging: false },
            );
          });
        }
      } catch (error) {
        logger.error(`Erro ao processar planilha: ${error}`);
        await this.repository.update(
          {
            status: 'error',
            message: error.message,
            importedAt: null,
          },
          {
            where: { idProcessesFile: file.idProcessesFile },
            returning: false,
          },
        );
      }
    }
  };

  findMaxContentLengthPerColumn = sheetData => {
    const maxContentLengths = [];

    sheetData.forEach(row => {
      row.forEach((cell, index) => {
        const cellValue = cell?.v ? cell.v : cell || '';

        if (index === row.length - 2) {
          maxContentLengths[index] = 15;
        } else {
          const longestSegment = cellValue
            .toString()
            .split('\n')
            .reduce((max, line) => {
              return line.length > max ? line.length : max;
            }, 0);
          if (index >= maxContentLengths.length) {
            maxContentLengths.push(longestSegment);
          } else if (longestSegment > maxContentLengths[index]) {
            maxContentLengths[index] = longestSegment;
          }
        }
      });
    });

    return maxContentLengths;
  };

  getFormattedFileName = (fileName, extension) => {
    let formattedName = fileName
      .replace(/ /g, '_')
      .replace(/[^a-zA-Z0-9_\-.]/g, '');
    return extension
      ? formattedName.replace(/\.[^/.]+$/, `.${extension}`)
      : formattedName;
  };

  getPriorityIdByDescriptionOrAbbreviation = str => {
    const patternsAndIds = [
      { key: ['Sem prioridade', '', undefined], value: 0 },
      { key: ['Idoso', 'Idosa(a) maior de 80 anos'], value: 4 },
      { key: ['Art. 1048, II', 'ECA'], value: 1 },
      { key: ['Art. 1048, IV', 'Licitação'], value: 2 },
      { key: ['Art. 7 - 12.016/2009'], value: 3 },
      { key: ['Doença grave', 'Portador(a) de doença grave'], value: 7 },
      { key: ['Deficiente', 'Pessoa com deficiencia'], value: 5 },
      { key: ['Situação rua', 'Pessoa em situação de rua'], value: 6 },
      { key: ['Réu Preso', 'Réu preso', 'preso'], value: 8 },
    ];

    for (const pattern of patternsAndIds) {
      if (pattern.key.some(substring => str === substring)) {
        return pattern.value;
      }
    }

    return null;
  };

  validateRecord = record => {
    if (typeof record === 'number') record = record.toString();
    const filteredRecord = record.replace(/[^\d]/g, '');
    const regexFilter = /^\d{20}$/;
    const isRecordValid = regexFilter.test(filteredRecord);

    return {
      filteredRecord,
      valid: isRecordValid,
    };
  };

  buildFileFilters(req) {
    const filter = {};
    const { nameOrRecord } = req.query;
    if (nameOrRecord) {
      const filterValue = `%${nameOrRecord}%`;
      filter[Op.or] = [
        { name: { [Op.iLike]: filterValue } },
        { fileName: { [Op.iLike]: filterValue } },
        { '$fileItems.record$': { [Op.iLike]: filterValue } },
      ];
    }
    return filter;
  }

  buildFileItemsFilters(req) {
    const filter = {};
    const { idProcessesFile, filter: mainFieldsFilter } = req.query;
    if (idProcessesFile) filter.idProcessesFile = idProcessesFile;
    if (mainFieldsFilter) {
      const filterValue = `%${mainFieldsFilter}%`;
      filter[Op.or] = [
        { record: { [Op.iLike]: filterValue } },
        { nickname: { [Op.iLike]: filterValue } },
        { priority: { [Op.iLike]: filterValue } },
        { flow: { [Op.iLike]: filterValue } },
      ];
    }
    return filter;
  }

  isFileType = (fileName, extension) =>
    this.extractExtensionFromFileName(fileName) === extension;

  extractExtensionFromFileName = fileName =>
    fileName.split('.').pop().toLowerCase();

  convertCsvBufferToXlsx = async (dataOriginalFile, originalFileName) => {
    const relativePath = './tempImportationFiles';

    const uniqueId = uuidv4().split('-')[0];

    const tempCsvFileName = uniqueId + '_' + originalFileName;
    const tempCsvFilePath = path.join(relativePath, tempCsvFileName);
    const tempXlsxFilePath = tempCsvFilePath.replace('.csv', '.xlsx');

    await fs.writeFile(tempCsvFilePath, dataOriginalFile);

    await convertCsvToXlsx(tempCsvFilePath, tempXlsxFilePath);

    const xlsxBuffer = await fs.readFile(tempXlsxFilePath);

    await fs.unlink(tempCsvFilePath);
    await fs.unlink(tempXlsxFilePath);

    return xlsxBuffer;
  };

  convertXlsxToCsv = buffer => {
    const bufferData = Buffer.from(buffer);
    const workbook = XLSX.read(bufferData, { type: 'buffer' });
    const firstSheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[firstSheetName];
    return Buffer.from(XLSX.utils.sheet_to_csv(worksheet));
  };
}
