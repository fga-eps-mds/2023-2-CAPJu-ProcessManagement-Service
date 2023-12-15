'use strict';

module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.removeColumn('processesFile', 'dataResultingFile');
  },

  async down(queryInterface, Sequelize) {
    await queryInterface.addColumn('processesFile', 'dataResultingFile', {
      type: Sequelize.BLOB,
      allowNull: true,
    });
  },
};
