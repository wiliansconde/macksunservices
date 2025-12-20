"""
// Conecta ao banco craam_master
const dbTarget = db.getSiblingDB("craam_master");

// Lista de coleções a limpar
const collections = [
  "exported_files_to_cloud",
  "global_data_statistics",
  "partition_map",
  "processed_file_trace",
  "queue_file_ingestion",
  "queue_generate_file_to_export_to_cloud"
];

// Apaga os documentos de cada coleção
collections.forEach(function(collName) {
  const result = dbTarget[collName].deleteMany({});
  print(`Apagados ${result.deletedCount} documentos da coleção ${collName}`);
});

print("Todas as coleções foram esvaziadas com sucesso.");



"""