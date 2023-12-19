'use strict';

const { DataTypes } = require('sequelize');

module.exports = {
  async up(queryInterface) {
    await queryInterface.changeColumn('process', 'nickname', {
      type: DataTypes.STRING(255),
      allowNull: true,
    });
  },

  async down(queryInterface) {
    await queryInterface.changeColumn('process', 'nickname', {
      type: DataTypes.STRING(255),
      allowNull: false,
    });
  },
};
