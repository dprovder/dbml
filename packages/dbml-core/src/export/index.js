import ModelExporter from './ModelExporter';
import Parser from '../parse/Parser';
import DbtExporter from './DbtExporter';
import TransformExporter from './TransformExporter';

function _export (str, format) {
  const database = (new Parser()).parse(str, 'dbmlv2');
  return ModelExporter.export(database.normalize(), format);
}

export default {
  export: _export,
  DbtExporter,
  TransformExporter,
};
