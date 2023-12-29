const express = require("express");
const APS = require("forge-apis");
const SupabaseCli = require("@supabase/supabase-js");
const cors = require("cors");
const axios = require("axios");
const decompress = require("decompress");
const fs = require("fs");
const reader = require("xlsx");

const app = express();
const port = 3009;
const FORGE_CLIENT_ID = [
  "XOkLWtB5kBHroW7ggy0GNfDrApti069T",
  "WMbDYlPoLcxK2pg8HqfyA4hSPtAgh3EC",
  "yuwWnOEIiywo3kAaZLfCvS5mCfuGfZbX",
  "DbB5G5DSkvg8jiagyum0noQWvqdy1KcB",
];
const FORGE_CLIENT_SECRET = [
  "pHivJw7f5LxUGACT",
  "cgPOxr256xsCh9nx",
  "MigAJfFxyqROkSf3",
  "s55HlTfDWPkQ8D5X",
];

const SUPABASE_URL = "https://rgeqmdmjoxtmidxmrcpb.supabase.co";
const SUPABASE_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJnZXFtZG1qb3h0bWlkeG1yY3BiIiwicm9sZSI6ImFub24iLCJpYXQiOjE2Nzk3NTk2NTcsImV4cCI6MTk5NTMzNTY1N30.vWLo2yH7y3Ej4bwnfOhkg0CV_1kzGN2LARnLegpBwqE";
const SUPABASE_MASTER_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJnZXFtZG1qb3h0bWlkeG1yY3BiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTY3OTc1OTY1NywiZXhwIjoxOTk1MzM1NjU3fQ.0n--YNbiwRgj-yfBg2z4To5SRXvNyaOKNMUpRYnmtH0";
app.use(express.json());
app.use(
  cors({
    origin: "*",
  })
);

app.post("/process-drawings", async (req, res) => {
  try {
    const supabase = SupabaseCli.createClient(SUPABASE_URL, SUPABASE_TOKEN);

    const { data: drawings, error } = await supabase
      .from("Drawings_v2_source")
      .select("*");

    if (error) {
      throw new Error("Failed to fetch drawings");
    }

    const batchCount = 100; // Number of records to process in each batch
    const delayBetweenBatches = 60000; // Delay between batches in milliseconds (1 minute in this case)

    async function processDrawings(startIndex) {
      const endIndex = Math.min(startIndex + batchCount, drawings.length);
      for (let i = startIndex; i < endIndex; i++) {
        const drawing = drawings[i];
        try {
          // Your API call logic here using 'drawing'
          // Example: Make API call using drawing.id and drawing.file

          console.log(
            `Making API call for drawing with ID: ${drawing.id} and file: ${drawing.file}`
          );
          const fileName = drawing.file;

          const { data, error } = await supabase.storage
            .from("project-documents")
            .download(fileName);
          console.log("FILE DOWNLOADED");
          let oAuth2TwoLegged = new APS.AuthClientTwoLegged(
            FORGE_CLIENT_ID[3],
            FORGE_CLIENT_SECRET[3],
            [
              "bucket:create",
              "bucket:read",
              "data:read",
              "data:write",
              "data:create",
            ],
            true
          );
          await oAuth2TwoLegged.authenticate();
          const credentials = oAuth2TwoLegged.getCredentials();
          console.log("CREDENTIALS GENERATED ==", credentials);
          const size = data?.size ?? 0;
          const buffer = await data?.arrayBuffer();
          var asd = await new APS.ObjectsApi()
            .uploadObject(
              FORGE_CLIENT_ID[3].toLowerCase() + "-basic-app",
              fileName,
              size,
              buffer,
              {},
              oAuth2TwoLegged,
              credentials
            )
            .catch((uploadObjectError) => {
              console.log("UPLOAD ERROR", uploadObjectError);
              res.send(uploadObjectError.toString());
            });
          console.log("FILE UPLOADED==", asd);
          const urn = Buffer.from(asd.body.objectId)
            .toString("base64")
            .replace(/=/g, "");
          console.log("URN", urn);
          const job = {
            input: { urn },
            output: { formats: [{ type: "svf2", views: ["2d", "3d"] }] },
          };
          const resp = await new APS.DerivativesApi()
            .translate(job, {}, null, credentials)
            .catch((e) => {
              console.log("ERROR=>", e);
            });
          const { error1 } = await supabase
            .from("Drawings_v2_source")
            .update({ config: urn, creds: 3, migration_status: 1 })
            .eq("id", drawing.id);
          console.log("TRANSLATION COMPLETED", resp);
          // return resp.body;
          res.send(resp.body);
        } catch (error) {
          console.error(
            `Error processing drawing with ID: ${drawing.id}`,
            error
          );
          // Handle errors if needed
        }
      }

      // If there are more drawings to process, schedule the next batch after a delay
      if (endIndex < drawings.length) {
        setTimeout(() => processDrawings(endIndex), delayBetweenBatches);
      }
    }

    // Start processing drawings from index 0
    processDrawings(0);

    res.status(200).send({ message: "Processing started" });
  } catch (error) {
    res.status(500).send({ error: "Internal Server Error" });
  }
});
app.post("/upload-file-to-forge", async (req, res) => {
  console.log(req.body.record);
  const fileName = req.body.record.file;
  if (
    req.body.type === "UPDATE" &&
    req.body.record.file === req.body.old_record.file
  ) {
    res.send({ message: "No change in file" });
    return;
  }
  const supabase = SupabaseCli.createClient(SUPABASE_URL, SUPABASE_TOKEN);
  const { data, error } = await supabase.storage
    .from("project-documents")
    .download(fileName);
  console.log("FILE DOWNLOADED");
  let oAuth2TwoLegged = new APS.AuthClientTwoLegged(
    FORGE_CLIENT_ID[3],
    FORGE_CLIENT_SECRET[3],
    ["bucket:create", "bucket:read", "data:read", "data:write", "data:create"],
    true
  );
  await oAuth2TwoLegged.authenticate();
  const credentials = oAuth2TwoLegged.getCredentials();
  console.log("CREDENTIALS GENERATED ==", credentials);
  const size = data?.size ?? 0;
  const buffer = await data?.arrayBuffer();
  var asd = await new APS.ObjectsApi()
    .uploadObject(
      FORGE_CLIENT_ID[3].toLowerCase() + "-basic-app",
      fileName,
      size,
      buffer,
      {},
      oAuth2TwoLegged,
      credentials
    )
    .catch((uploadObjectError) => {
      console.log("UPLOAD ERROR", uploadObjectError);
      res.send(uploadObjectError.toString());
    });
  console.log("FILE UPLOADED==", asd);
  const urn = Buffer.from(asd.body.objectId)
    .toString("base64")
    .replace(/=/g, "");
  console.log("URN", urn);
  const job = {
    input: { urn },
    output: { formats: [{ type: "svf", views: ["2d", "3d"] }] },
  };
  const resp = await new APS.DerivativesApi()
    .translate(job, {}, null, credentials)
    .catch((e) => {
      console.log("ERROR=>", e);
    });
  const { error1 } = await supabase
    .from("Drawings_v2_source")
    .update({ config: urn, creds: 3 })
    .eq("id", req.body.record.id);
  console.log("TRANSLATION COMPLETED", resp);
  // return resp.body;
  res.send(resp.body);
});

app.post("/getDrawingDetails/:id", async (req, res) => {
  const supabase = SupabaseCli.createClient(SUPABASE_URL, SUPABASE_TOKEN);
  var { data, error1 } = await supabase
    .from("Drawings_v2_source")
    .select(
      "*,project_disciplines(*),Drawing_Versions(version_number,created_at::date),Projects(project_code,pin),drawing_project_levels_v2_source!fk_drawing_id(project_levels(short_code)),project_file_types(code),project_deliverables(code),project_buildings(code)"
    )
    .eq("code", req.params.id);
  if (data.length === 0) {
    res.send({ success: false, message: "Drawing not found" });
    return;
  }
  if (
    ((data[0].Projects.pin && data[0].Projects.pin !== req.body.pin) ||
      data[0].pin !== req.body.pin) &&
    data[0].pin !== null
  ) {
    res.send({
      message: "PIN is wrong!",
    });
    return;
  }

  const drawing = data[0];
  var { data, error } = await supabase
    .from("Organization")
    .select()
    .eq("id", drawing.organization_id);
  organization = data[0];
  var { data, error } = supabase.storage
    .from("project-logo")
    .getPublicUrl(organization.logo);
  organization.logo_url = data.publicUrl;
  console.log("DATA", data);

  let oAuth2TwoLegged = new APS.AuthClientTwoLegged(
    FORGE_CLIENT_ID[drawing.creds],
    FORGE_CLIENT_SECRET[drawing.creds],
    ["bucket:create", "bucket:read", "data:read", "data:write", "data:create"],
    true
  );
  await oAuth2TwoLegged.authenticate();
  const credentials = oAuth2TwoLegged.getCredentials();
  res.send({
    drawing: drawing,
    organization: organization,
    token: credentials.access_token,
    drawing_code: getDrawingCode(organization, drawing),
    version_text: getVersionText(drawing),
  });
});

app.post("/getProjectDetails/:id", async (req, res) => {
  const supabase = SupabaseCli.createClient(SUPABASE_URL, SUPABASE_TOKEN);
  var { data, error } = await supabase
    .from("Projects")
    .select()
    .eq("code", req.params.id);

  if (error) {
    res.send({
      message: "Project not found!",
    });
    return;
  }
  project = data[0];
  console.log(!(project.pin == req.body.pin || project.pin === null));
  if (!(project.pin == req.body.pin || project.pin === null)) {
    res.send({
      message: "PIN is wrong!",
    });
    return;
  }
  var { data, error1 } = await supabase
    .from("Drawings_v2_source")
    .select()
    .eq("project_id", project.id);
  res.send({
    project: project,
    drawings: data,
  });
});

app.post("/url-shortener", async (req, res) => {
  const url = req.body.record.url;
  const table = req.body.table.toLowerCase();
  var updateObject = {};
  var uniqueCOde = {};
  if (req.body.record.code === undefined || req.body.record.code === null) {
    uniqueCOde = Math.floor(100000 + Math.random() * 900000);
    updateObject.code = uniqueCOde;
  } else {
    uniqueCOde = req.body.record.code;
  }
  if (
    req.body.record.organization_id === undefined ||
    req.body.record.organization_id === null
  ) {
    res.send({ success: true });
    return;
  }
  console.log("HELLO",req.body);
  const supabase = SupabaseCli.createClient(SUPABASE_URL, SUPABASE_TOKEN);
  var { data, error } = await supabase
    .from("Organization")
    .select()
    .eq("id", req.body.record.organization_id);
  var newURL = "";
  if (table == "drawings_v2_source") {
    newURL = data[0].domain + uniqueCOde ?? req.body.record.code;
  } else {
    newURL = data[0].domain + "p/" + uniqueCOde ?? req.body.record.code;
  }
  updateObject.url = newURL;
  console.log("UPDATE OBJECT", updateObject);
  if (updateObject.url === req.body.record.url) {
    res.send({ success: true });
    return;
  }
  var { error1 } = supabase
    .from(req.body.table)
    .update(updateObject)
    .eq("id", req.body.record.id)
    .then((result) => {
      console.log("RESULT FROM SUPABASE", result);
    });
  res.send({ success: true });
});

app.post("/startImport", async (req, res) => {
  const importObject = req.body.record;
  const supabase = SupabaseCli.createClient(
    SUPABASE_URL,
    SUPABASE_MASTER_TOKEN
  );

  const tempDir = "./temp/";
  const unzipDir = "./unzip/";
  var constTypes = [];
  var { data: types, error: error12 } = await supabase
    .from("Drawing_Types")
    .select();
  types.forEach((type) => {
    constTypes[type.name.toLowerCase()] = type.id;
  });
  const { data: excelFile, error: error1 } = await supabase.storage
    .from("project-documents")
    .download("import/excel/" + importObject.excel_file);

  const excelFilename = tempDir + importObject.excel_file;
  const bufferExcel = Buffer.from(await excelFile.arrayBuffer());
  await fs.promises.writeFile(excelFilename, bufferExcel);

  const excelReader = reader.readFile(excelFilename);
  const sheets = excelReader.SheetNames;
  let importData = [];
  for (let i = 0; i < sheets.length; i++) {
    const temp = reader.utils.sheet_to_json(
      excelReader.Sheets[excelReader.SheetNames[i]]
    );
    temp.forEach((res) => {
      importData.push(res);
    });
  }

  //   console.log(importData);

  const { data, error } = await supabase.storage
    .from("project-documents")
    .download("import/zip/" + importObject.zip_file);

  console.log("ERROR", error);
  const blob = data;
  const fileName = tempDir + importObject.zip_file;
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }
  if (!fs.existsSync("./unzip")) {
    fs.mkdirSync("./unzip");
  }
  const buffer = Buffer.from(await blob.arrayBuffer());
  const unzipPath = `./unzip/${importObject.id}/`;
  await fs.promises.writeFile(fileName, buffer);
  if (!fs.existsSync(unzipPath)) {
    fs.mkdirSync(unzipPath);
  }

  await decompress(fileName, unzipPath)
    .then((files) => {
      files.forEach(async (file) => {
        var foundValue = importData.filter((obj) => obj.file === file.path);

        if (foundValue != null) {
          const { data, error } = await supabase.storage
            .from("project-documents")
            .upload(file.path, file.data, {
              cacheControl: "3600",
              upsert: false,
            });

          // Get Project Level
          let levels = foundValue[0].level;
          let level = [];
          if (levels !== undefined) level = levels.split(", ");

          let level_ids = await supabase
            .from("project_levels")
            .select("*")
            .eq("project_id", importObject.project_id)
            .in(
              "name",
              level.map((value) => value)
            )
            .then((res) => {
              // const map = new Map();
              let id = res.data.map((level) => {
                // console.log("res ->", level);
                return level.id;
              });
              return id;
            })
            .catch((error) => console.log(error));

          //Get Descipline id of Project

          let descipline_id = await supabase
            .from("project_disciplines")
            .select("*")
            .eq("project_id", importObject.project_id)
            .eq("discipline_name", foundValue[0].disciplines)
            .then((res) => {
              //   console.log("res ->", res);
              if (res.data.length !== 0) return res.data[0].id;
            })
            .catch((error) => console.log(error));

          // Get File Type Id for Project Drawing

          let file_type_id = await supabase
            .from("project_file_types")
            .select("*")
            .eq("project_id", importObject.project_id)
            .eq("name", foundValue[0].file_type)
            .then((res) => {
              //   console.log("res ->", res);
              return res.data[0].id;
            })
            .catch((error) => console.log(error));

          // Get Deliverables id for project Drawing

          let deliverable_id = await supabase
            .from("project_deliverables")
            .select("*")
            .eq("project_id", importObject.project_id)
            .eq("deliverable_name", foundValue[0].deliverables)
            .then((res) => {
              //   console.log("res ->", res);
              if (res.data.length !== 0) return res.data[0].id;
            })
            .catch((error) => console.log(error));

          // Date format

          let date;

          if (foundValue[0].date) {
            date = foundValue[0].date;
          } else {
            const dateObject = new Date();
            date = dateObject.toISOString().split("T")[0];
          }

          let { data: drawing, error: drawingError } = await supabase
            .from("Drawings_v2_source")
            .insert([
              {
                name: foundValue[0].name,
                description: foundValue[0].description,
                file: foundValue[0].file,
                organization_id: importObject.organization_id,
                project_id: importObject.project_id,
                is_active: true,
                descipline_id,
                file_type_id,
                deliverable_id,
                drawing_date: date,
              },
            ])
            .select()
            .single();
          console.log("DRAWING INSERTED");
          //   console.log("TYPE", foundValue[0].type.toLowerCase());
          //   console.log("type:", constTypes[foundValue[0].type.toLowerCase()]);
          console.log("DRAWING ERROR", drawingError);
          let { data: version, error: versionError } = await supabase
            .from("Drawing_Versions")
            .insert([
              {
                version_number: 1,
                drawing_id: drawing.id,
                file: drawing.file,
                description: drawing.description,
                organization_id: drawing.organization_id,
              },
            ])
            .select()
            .single();
          console.log("VERSION ERROR INSERT", versionError);

          if (level_ids.length !== 0) {
            level_ids.map(async (level_id) => {
              let { data: drawing_levels, error: drawing_levels_error } =
                await supabase
                  .from("drawing_project_levels")
                  .insert([
                    {
                      drawing_id: drawing.id,
                      level_id,
                    },
                  ])
                  .select()
                  .single();
              console.log("DRAWING PROJECT LEVELS", drawing_levels_error);
            });
          }
          await supabase
            .from("Drawings_v2_source")
            .update({ current_version: version.id })
            .eq("id", drawing.id);
        }
      });
      res.send({ success: true });
    })

    .catch((error) => {
      console.log(error);
    });
});

app.post("/download", async (req, res) => {
  try {
    // console.log("headers ->", req.headers.authorization);
    // console.log("body ->", req.body);
    // name of model to download
    const name = "MyForgeModel";

    // URN of model to download
    // const urn = "dXGhsujdj .... ";

    // Get Forge service
    const forgeSvc = ServiceManager.getService("ForgeSvc");

    // getToken async function
    // const getToken = () => forgeSvc.get2LeggedToken();

    // Get Extractor service
    const extractorSvc = ServiceManager.getService("ExtractorSvc");

    // target path to download SVF
    const dir = path.resolve(__dirname, `${name}`);

    // perform download
    const files = await extractorSvc.download(
      req.headers.authorization,
      req.body.urn,
      dir
    );

    console.log(files);
  } catch (error) {
    console.log(error);
  }
});

function getVersionText(drawing) {
  return (
    "V" +
    drawing.Drawing_Versions?.version_number +
    "(" +
    drawing.Drawing_Versions?.created_at +
    ")"
  );
}

function getDrawingCode(organization, drawing) {
  let drawingCode = "";
  if (drawing.Projects?.project_code != null) {
    drawingCode += drawing.Projects.project_code;
  }
  drawingCode += "-";
  if (organization?.code != null) {
    drawingCode += organization.code;
  }
  drawingCode += "-";

  if (drawing.project_buildings?.code != null) {
    drawingCode += drawing.project_buildings?.code;
  }
  drawingCode += "-";

  if (drawing.drawing_project_levels_v2_source.length !== 0) {
    if (drawing.drawing_project_levels_v2_source.length > 1) {
      drawingCode += "ZZ";
    } else {
      drawingCode +=
        drawing.drawing_project_levels_v2_source[0].project_levels.short_code;
    }
  }
  drawingCode += "-";

  if (drawing.project_file_types?.code != null) {
    drawingCode += drawing.project_file_types?.code;
  }

  drawingCode += "-";

  if (drawing.project_deliverables?.code != null) {
    drawingCode += drawing.project_deliverables.code;
  }
  return drawingCode;
}

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
