import services from '../services/_index.js';
import { userFromReq } from '../../middleware/authMiddleware.js';

export class ProcessesFileController {
  constructor() {
    this.processesFileService = services.processesFileService;
  }

  findAllPaged = async (req, res) => {
    try {
      const data = await this.processesFileService.findAllPaged(req, res);
      return res.status(200).json(data);
    } catch (error) {
      console.log(error);
      return res
        .status(500)
        .json({ error: `${error}`, message: `Erro ao buscar arquivos` });
    }
  };

  findAllItemsPaged = async (req, res) => {
    try {
      const result = await this.processesFileService.findAllItemsPaged(
        req,
        res,
      );
      return res.status(200).json(result);
    } catch (error) {
      return res
        .status(500)
        .json({ error: `${error}`, message: `Erro ao buscar items` });
    }
  };

  create = async (req, res) => {
    try {
      const { cpf: importedBy } = await userFromReq(req);

      const { name } = req.body;

      const validFileTypes = [
        {
          type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          extension: 'xlsx',
        },
        { type: 'application/vnd.ms-excel', extension: 'xls' },
        { type: 'text/csv', extension: 'csv' },
        { type: 'application/csv', extension: 'csv' },
      ];

      const {
        name: fileName,
        data: dataOriginalFile,
        mimetype,
      } = req.files.file;

      if (!fileName || !dataOriginalFile) {
        return res.status(400).json({ message: `Arquivo não inserido.` });
      }

      const fileType = validFileTypes.find(
        validFileType => validFileType.type === mimetype,
      );

      if (!fileType) {
        return res
          .status(415)
          .json({ message: `Formato de arquivo inválido.` });
      }

      const data = await this.processesFileService.createFile({
        importedBy,
        dataOriginalFile,
        fileName: this.processesFileService.getFormattedFileName(
          fileName,
          fileType.extension,
        ),
        name,
        status: 'waiting',
      });

      return res
        .status(200)
        .json({ message: `Arquivo cadastrado com sucesso`, data });
    } catch (error) {
      return res.status(500).json({
        error: `${error}`,
        message: `Erro ao salvar remessa de processos`,
      });
    }
  };

  updateFileItem = async (req, res) => {
    try {
      const { idProcessesFileItem } = req.params;

      const newData = req.body;

      await this.processesFileService.updateFileItem(idProcessesFileItem, {
        ...newData,
        message: null,
      });

      return res.status(200).json({ idProcessesFileItem });
    } catch (error) {
      return res
        .status(500)
        .json({ error: `${error}`, message: `Erro ao atualizar item` });
    }
  };

  findFileById = async (req, res) => {
    try {
      const { idProcessesFile } = req.params;

      let { original = 'true', format = 'xlsx' } = req.query;

      const file = await this.processesFileService.findFileById(
        idProcessesFile,
        original === 'true',
        format,
      );

      return res.status(200).json(file);
    } catch (error) {
      return res.status(500).json({
        error: `${error}`,
        message: `Erro ao salvar remessa de processos`,
      });
    }
  };

  deleteById = async (req, res) => {
    const { idProcessesFile } = req.params;

    try {
      const result = await this.processesFileService.deleteFileById(
        idProcessesFile,
      );

      if (!result) {
        return res
          .status(404)
          .json({ error: `Não há registro ${req.params.record}.` });
      }

      return res
        .status(200)
        .json({ message: 'Arquivo e itens associados apagados.' });
    } catch (error) {
      return res
        .status(500)
        .json({ error, message: `Erro ao apagar arquivo: ${error}` });
    }
  };

  findById = async (req, res) => {
    try {
      const { idProcessesFile } = req.params;

      await this.processesFileService.findById(idProcessesFile);

      return res
        .status(200)
        .json({ message: `Arquivo cadastrado com sucesso` });
    } catch (error) {
      return res.status(500).json({
        error: `${error}`,
        message: `Erro ao salvar remessa de processos`,
      });
    }
  };

  generateResultingFile = async (req, res) => {
    try {
      const { idProcessesFile } = req.params;
      const data = await this.processesFileService.generateResultingFile(
        idProcessesFile,
      );
      return res.status(200).json(data);
    } catch (error) {
      return res.status(500).json({
        error: `${error}`,
        message: `Erro ao gerar arquivo resultado.`,
      });
    }
  };
}
