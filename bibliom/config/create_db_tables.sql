CREATE TABLE IF NOT EXISTS `author` (
  `idauthor` INT(11) NOT NULL AUTO_INCREMENT,
  `given_names` VARCHAR(500) NULL DEFAULT NULL,
  `last_name` VARCHAR(500) NULL DEFAULT NULL,
  `h-index` INT(11) NULL DEFAULT NULL,
  `orcid` VARCHAR(500) NULL DEFAULT NULL,
  `corporate` INT(1) NULL DEFAULT '0',
  PRIMARY KEY (`idauthor`),
  UNIQUE INDEX `orcid_UNIQUE` (`orcid` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `journal` (
  `idjournal` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(1000) NULL DEFAULT NULL,
  `issn` VARCHAR(9) NULL DEFAULT NULL,
  `essn` VARCHAR(9) NULL DEFAULT NULL,
  `short_title` VARCHAR(1000) NULL DEFAULT NULL,
  PRIMARY KEY (`idjournal`),
  UNIQUE INDEX `issn_UNIQUE` (`issn` ASC),
  UNIQUE INDEX `essn_UNIQUE` (`essn` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `paper` (
  `doi` VARCHAR(150) NULL DEFAULT NULL,
  `title` VARCHAR(1000) NULL DEFAULT NULL,
  `publication_date` DATE NULL DEFAULT NULL,
  `abstract` TEXT NULL DEFAULT NULL,
  `open_access` TINYINT(1) NULL DEFAULT NULL,
  `url` VARCHAR(2083) NULL DEFAULT NULL,
  `idjournal` INT(11) NULL DEFAULT NULL,
  `idpaper` INT(11) NOT NULL AUTO_INCREMENT,
  `first_page` VARCHAR(10) NULL DEFAULT NULL,
  `last_page` VARCHAR(10) NULL DEFAULT NULL,
  `time_added` DATETIME NULL DEFAULT NULL,
  `content` LONGTEXT NULL DEFAULT NULL,
  `cited_records` LONGTEXT NULL DEFAULT NULL,
  `wos_identifier` VARCHAR(150) NULL DEFAULT NULL,
  `total_citations` INT(11) NULL DEFAULT NULL,
  `citation_record` LONGTEXT NULL DEFAULT NULL,
  `retracted_year` YEAR(4) NULL DEFAULT NULL,
  `citation_history` TEXT NULL DEFAULT NULL,
  PRIMARY KEY (`idpaper`),
  UNIQUE INDEX `idx_paper_doi` (`doi` ASC),
  UNIQUE INDEX `wos_identifier_UNIQUE` (`wos_identifier` ASC),
  INDEX `idjournal_idx` (`idjournal` ASC),
  CONSTRAINT `fk_paper_journal`
    FOREIGN KEY (`idjournal`)
    REFERENCES `journal` (`idjournal`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `citation` (
  `source_id` INT(11) NOT NULL,
  `target_id` INT(11) NOT NULL,
  PRIMARY KEY (`source_id`, `target_id`),
  INDEX `fk_citation_paper2_idx` (`target_id` ASC),
  CONSTRAINT `fk_citation_paper1`
    FOREIGN KEY (`source_id`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_citation_paper2`
    FOREIGN KEY (`target_id`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `modularity_measure` (
  `measure` VARCHAR(45) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(1024) NULL DEFAULT NULL,
  PRIMARY KEY (`measure`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `modularity_class` (
  `idmodularity_class` INT(11) NOT NULL AUTO_INCREMENT,
  `measure` VARCHAR(45) NOT NULL,
  `classification` INT(11) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`idmodularity_class`),
  UNIQUE INDEX `uq_measure_classification` (`measure` ASC, `classification` ASC),
  INDEX `fk_modularity_class_modularity_measure_idx` (`measure` ASC),
  CONSTRAINT `fk_modularity_class_modularity_measure`
    FOREIGN KEY (`measure`)
    REFERENCES `modularity_measure` (`measure`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `network` (
  `network_key` VARCHAR(45) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(1024) NULL DEFAULT NULL,
  `ref_column` VARCHAR(45) NOT NULL DEFAULT 'idpaper',
  `directed` TINYINT(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`network_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `network_edges` (
  `idnetwork_edges` INT(11) NOT NULL AUTO_INCREMENT,
  `network_key` VARCHAR(45) NULL DEFAULT NULL,
  `source` INT(11) NULL DEFAULT NULL,
  `target` INT(11) NULL DEFAULT NULL,
  `weight` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`idnetwork_edges`),
  UNIQUE INDEX `uq_network_source_target` (`network_key` ASC, `source` ASC, `target` ASC),
  INDEX `fk_network_edges_network_idx` (`network_key` ASC),
  INDEX `fk_network_edges_source_paper_idx` (`source` ASC),
  INDEX `fk_network_edges_target_paper_idx` (`target` ASC),
  CONSTRAINT `fk_network_edges_network`
    FOREIGN KEY (`network_key`)
    REFERENCES `network` (`network_key`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_network_edges_source_paper`
    FOREIGN KEY (`source`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_network_edges_target_paper`
    FOREIGN KEY (`target`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `paper_author` (
  `idauthor` INT(11) NOT NULL,
  `idpaper` INT(11) NOT NULL,
  PRIMARY KEY (`idauthor`, `idpaper`),
  INDEX `idauthor_idx` (`idauthor` ASC),
  INDEX `idpaper_idx` (`idpaper` ASC),
  CONSTRAINT `fk_paper_author_author`
    FOREIGN KEY (`idauthor`)
    REFERENCES `author` (`idauthor`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_paper_author_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `paper_keyword` (
  `idpaper_keyword` INT(11) NOT NULL AUTO_INCREMENT,
  `keyword` VARCHAR(100) NULL DEFAULT NULL,
  `idpaper` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`idpaper_keyword`),
  INDEX `idpaper_idx` (`idpaper` ASC),
  CONSTRAINT `fk_paper_keyword_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `paper_modularity_class` (
  `idpaper` INT(11) NOT NULL,
  `idmodularity_class` INT(11) NOT NULL,
  PRIMARY KEY (`idpaper`, `idmodularity_class`),
  INDEX `fk_paper_modularity_class_paper_idx` (`idpaper` ASC),
  INDEX `fk_paper_modularity_class_modularity_class_idx` (`idmodularity_class` ASC),
  CONSTRAINT `fk_paper_modularity_class_modularity_class`
    FOREIGN KEY (`idmodularity_class`)
    REFERENCES `modularity_class` (`idmodularity_class`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_paper_modularity_class_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

